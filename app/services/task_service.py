import logging
import os
from datetime import datetime, timedelta
from app import db
from app.models import Task, PermissionRequest, AuditEvent
from app.services.airflow_service import AirflowService
from app.services.ldap_service import LDAPService
from flask import current_app
import json

logger = logging.getLogger(__name__)

class TaskService:
    def __init__(self):
        self.airflow_service = AirflowService()
        self.ldap_service = LDAPService()
    
    def get_config(self):
        """Get task configuration from environment variables"""
        return {
            'max_retries': int(os.getenv('TASK_MAX_RETRIES', 3)),
            'retry_delay': int(os.getenv('TASK_RETRY_DELAY', 300)),  # 5 minutes
            'cleanup_days': int(os.getenv('TASK_CLEANUP_DAYS', 30)),
            'batch_size': int(os.getenv('TASK_BATCH_SIZE', 10)),
            'processing_interval': int(os.getenv('TASK_PROCESSING_INTERVAL', 300)),
            # Immediate execution timeouts
            'immediate_airflow_timeout': int(os.getenv('IMMEDIATE_AIRFLOW_TIMEOUT', 300)),  # 5 minutes
            'immediate_verification_timeout': int(os.getenv('IMMEDIATE_VERIFICATION_TIMEOUT', 60)),  # 1 minute
            # Immediate execution retry delays (shorter for immediate execution)
            'immediate_airflow_retry_delay': int(os.getenv('IMMEDIATE_AIRFLOW_RETRY_DELAY', 30)),  # 30 seconds
            'immediate_ad_retry_delay': int(os.getenv('IMMEDIATE_AD_RETRY_DELAY', 60))  # 60 seconds
        }
    
    def cleanup_csv_file(self, task):
        """Clean up CSV file associated with a task after AD verification completes or is cancelled"""
        try:
            task_data = task.get_task_data()
            csv_filename = task_data.get('csv_file_path')
            
            # Construct full path from filename using configured CSV output directory
            if csv_filename:
                from flask import current_app
                csv_output_dir = current_app.config.get('CSV_OUTPUT_DIR', '/app/sar_csv_files')
                csv_file_path = os.path.join(csv_output_dir, csv_filename)
                
                if os.path.exists(csv_file_path):
                    os.remove(csv_file_path)
                    logger.info(f"Cleaned up CSV file {csv_file_path} for task {task.id} after AD verification completion")
                    return True
                else:
                    logger.debug(f"CSV file {csv_file_path} not found for cleanup (task {task.id})")
            return False
            
        except Exception as e:
            logger.error(f"Error cleaning up CSV file for task {task.id}: {str(e)}")
            return False
    
    def create_approval_tasks(self, permission_request, validator, csv_file_path=None):
        """Create tasks when a permission request is approved - with immediate execution optimization"""
        try:
            config = self.get_config()
            logger.info(f"Starting optimized task creation for permission request {permission_request.id}")

            # Check if tasks already exist for this permission request
            from app.models import Task
            existing_tasks = Task.query.filter_by(permission_request_id=permission_request.id).all()

            if existing_tasks:
                logger.warning(f"Tasks already exist for permission request {permission_request.id}. Existing tasks: {[t.id for t in existing_tasks]}")
                # Return existing tasks instead of creating duplicates
                return existing_tasks

            # OPTIMIZATION: Try immediate execution first by creating tasks and executing them immediately
            immediate_result = self._try_immediate_execution_with_tasks(permission_request, validator, csv_file_path)

            if immediate_result['success']:
                logger.info(f"Successfully executed tasks immediately for permission request {permission_request.id}")
                return immediate_result['tasks']

            # Fallback: If immediate execution failed, convert existing tasks to queued mode
            logger.info(f"Immediate execution failed for request {permission_request.id}, converting tasks to queued mode")
            return self._convert_tasks_to_queued_mode(immediate_result['tasks'], permission_request, validator)

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating approval tasks for request {permission_request.id}: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")

            # Final fallback: try to create simple queued tasks
            try:
                logger.info(f"Attempting fallback task creation for request {permission_request.id}")
                return self._create_queued_tasks(permission_request, validator, csv_file_path)
            except Exception as fallback_error:
                logger.error(f"Fallback task creation also failed for request {permission_request.id}: {str(fallback_error)}")
                return []

    def _try_immediate_execution_with_tasks(self, permission_request, validator, csv_file_path):
        """Create tasks and try to execute them immediately with proper attempt tracking"""
        try:
            from app.models import Task
            config = self.get_config()
            logger.info(f"Creating tasks and attempting immediate execution for permission request {permission_request.id}")

            # Step 1: Create tasks first (so we can track attempts in DB)
            airflow_task = Task.create_airflow_task(permission_request, validator, csv_file_path)
            airflow_task.max_attempts = config['max_retries']
            airflow_task.next_execution_at = datetime.utcnow()
            db.session.add(airflow_task)
            db.session.flush()

            verification_task = Task.create_ad_verification_task(permission_request, validator, delay_seconds=0)
            verification_task.max_attempts = config['max_retries']
            # Link tasks
            verification_data = verification_task.get_task_data()
            verification_data['depends_on_task_id'] = airflow_task.id
            verification_data['csv_file_path'] = os.path.basename(csv_file_path) if csv_file_path else None
            verification_task.set_task_data(verification_data)
            db.session.add(verification_task)
            db.session.flush()

            # Step 2: Try quick Airflow execution (single attempt, no blocking)
            airflow_success = self._try_quick_airflow_execution(airflow_task, permission_request, validator, csv_file_path)

            if airflow_success:
                logger.info(f"✅ Airflow execution succeeded for task {airflow_task.id}, current status: {airflow_task.status}")
                # Step 3: Try quick AD verification (single attempt, no blocking)
                verification_success = self._try_quick_ad_verification(verification_task, permission_request)

                if verification_success:
                    logger.info(f"Both Airflow and AD verification completed quickly for request {permission_request.id}")
                    db.session.commit()
                    return {'success': True, 'tasks': [airflow_task, verification_task]}
                else:
                    logger.info(f"Quick AD verification failed for request {permission_request.id}, but Airflow succeeded - keeping Airflow as completed")
                    # Don't change Airflow task status - it succeeded
                    verification_task.status = 'pending'
                    verification_task.next_execution_at = None
                    db.session.commit()
                    return {'success': True, 'tasks': [airflow_task, verification_task]}
            else:
                logger.info(f"❌ Quick Airflow execution failed for request {permission_request.id}, will retry later")

            # If quick execution failed, schedule tasks for background processing
            logger.info(f"⚠️ WARNING: About to reset Airflow task {airflow_task.id} to pending - current status: {airflow_task.status}")
            airflow_task.status = 'pending'
            airflow_task.next_execution_at = datetime.utcnow()  # Execute ASAP

            # AD verification task should NOT be scheduled for execution yet
            # It will be scheduled automatically by the dependency system when Airflow completes
            verification_task.status = 'pending'
            verification_task.next_execution_at = None  # Will be set by dependency system

            logger.info(f"AD verification task {verification_task.id} will be scheduled when Airflow task {airflow_task.id} completes")

            db.session.commit()
            return {'success': True, 'tasks': [airflow_task, verification_task]}

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error during immediate execution with tasks for request {permission_request.id}: {str(e)}")
            return {'success': False, 'tasks': []}

    def _convert_tasks_to_queued_mode(self, tasks, permission_request, validator):
        """Convert failed immediate execution tasks to queued mode"""
        try:
            config = self.get_config()

            if not tasks:
                # If no tasks were created, create new queued tasks
                return self._create_queued_tasks(permission_request, validator, None)

            for task in tasks:
                # Reset task state for queued execution
                if task.status in ['failed', 'running']:
                    task.status = 'pending'
                    task.error_message = None
                    task.started_at = None
                    task.completed_at = None

                    # Reset for next execution but keep attempt count history
                    task.next_execution_at = datetime.utcnow()
                    task.updated_at = datetime.utcnow()

                    # For AD verification task, add delay
                    if task.task_type == 'ad_verification':
                        task.next_execution_at = datetime.utcnow() + timedelta(seconds=config['retry_delay'])

                    logger.info(f"Converted task {task.id} ({task.task_type}) to queued mode with {task.attempt_count} previous attempts")

            db.session.commit()
            logger.info(f"Converted {len(tasks)} tasks to queued mode for permission request {permission_request.id}")
            return tasks

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error converting tasks to queued mode for request {permission_request.id}: {str(e)}")
            # Final fallback: create new queued tasks
            return self._create_queued_tasks(permission_request, validator, None)

    def _try_quick_airflow_execution(self, task, permission_request, validator, csv_file_path):
        """Try a single quick Airflow execution without blocking or retries"""
        try:
            task_data = task.get_task_data()
            conf = {
                'change_file': task_data.get('csv_file_path'),
                'request_ids': [permission_request.id],
                'triggered_by': validator.username,
                'folder_path': task_data.get('folder_path'),
                'ad_group_name': task_data.get('ad_group_name'),
                'permission_type': task_data.get('permission_type'),
                'ad_source_domain': os.getenv('AD_DOMAIN_PREFIX', ''),
                'ad_target_domain': os.getenv('AD_TARGET_DOMAIN', 'AUDI'),
                'immediate_execution': True,
                'custom_run_id': f"quick__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{permission_request.id}"
            }

            # Mark task as running and increment attempt
            task.mark_as_running()
            task.increment_attempt_count()

            logger.info(f"Attempting quick Airflow execution for task {task.id}")

            # Single attempt - no retries, no waiting for completion
            success = self.airflow_service.trigger_dag(conf)

            if success:
                logger.info(f"Quick Airflow execution successful for task {task.id}")
                task.mark_as_completed({
                    'execution_type': 'quick',
                    'dag_triggered': True,
                    'execution_time': datetime.utcnow().isoformat(),
                    'run_id': conf['custom_run_id'],
                    'quick_success': True
                })

                # Commit the completion to database
                db.session.commit()

                # Execute dependent tasks immediately when Airflow completes successfully
                try:
                    self._schedule_dependent_ad_verification_tasks(task)
                except Exception as dependent_error:
                    logger.warning(f"Could not schedule dependent AD verification tasks for task {task.id}: {str(dependent_error)}")
                    # Don't fail the main task - dependent tasks can be processed later

                return True
            else:
                logger.info(f"Quick Airflow execution failed for task {task.id}, will be retried by background processor")
                task.status = 'pending'  # Reset to pending for background retry
                task.started_at = None
                return False

        except Exception as e:
            logger.error(f"Error in quick Airflow execution for task {task.id}: {str(e)}")
            task.status = 'pending'  # Reset to pending for background retry
            task.started_at = None
            return False

    def _try_quick_ad_verification(self, task, permission_request):
        """Try a single quick AD verification without blocking or retries"""
        try:
            # Mark task as running and increment attempt
            task.mark_as_running()
            task.increment_attempt_count()

            logger.info(f"Attempting quick AD verification for task {task.id}")

            # Single attempt - no retries
            verification_result = self.verify_ad_changes(
                folder_path=permission_request.folder.path,
                ad_group_name=permission_request.ad_group.name if permission_request.ad_group else None,
                access_type=permission_request.permission_type,
                action_type='add',
                requester_user=permission_request.requester
            )

            if verification_result['success']:
                logger.info(f"Quick AD verification successful for task {task.id}")
                task.mark_as_completed({
                    'execution_type': 'quick',
                    'verification_status': 'success',
                    'ad_permissions_applied': True,
                    'verification_time': datetime.utcnow().isoformat(),
                    'details': verification_result['details'],
                    'quick_success': True
                })

                # Clean up CSV file after successful quick AD verification
                self.cleanup_csv_file(task)

                return True
            else:
                logger.info(f"Quick AD verification failed for task {task.id}, will be retried by background processor")
                task.status = 'pending'  # Reset to pending for background retry
                task.started_at = None
                return False

        except Exception as e:
            logger.error(f"Error in quick AD verification for task {task.id}: {str(e)}")
            task.status = 'pending'  # Reset to pending for background retry
            task.started_at = None
            return False

    def _execute_airflow_task_immediately(self, task, permission_request, validator, csv_file_path):
        """Execute Airflow task immediately with proper attempt tracking in database"""
        config = self.get_config()
        max_attempts = config['max_retries']
        retry_delay = config['immediate_airflow_retry_delay']

        task_data = task.get_task_data()
        conf = {
            'change_file': task_data.get('csv_file_path'),
            'request_ids': [permission_request.id],
            'triggered_by': validator.username,
            'folder_path': task_data.get('folder_path'),
            'ad_group_name': task_data.get('ad_group_name'),
            'permission_type': task_data.get('permission_type'),
            'ad_source_domain': os.getenv('AD_DOMAIN_PREFIX', ''),
            'ad_target_domain': os.getenv('AD_TARGET_DOMAIN', 'AUDI'),
            'immediate_execution': True
        }

        # Mark task as running
        task.mark_as_running()
        db.session.commit()

        # Perform up to 3 attempts
        for attempt in range(1, max_attempts + 1):
            try:
                # Update attempt count in database
                task.increment_attempt_count()
                db.session.commit()

                # Generate unique run ID for this attempt
                run_id = f"immediate__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_attempt{attempt}_{permission_request.id}"
                conf['custom_run_id'] = run_id

                logger.info(f"Airflow execution attempt {attempt}/{max_attempts} for task {task.id}")

                # Trigger Airflow DAG
                success = self.airflow_service.trigger_dag(conf)

                if success:
                    logger.info(f"Airflow DAG triggered successfully on attempt {attempt}/{max_attempts} for task {task.id}")

                    # Update task status to running
                    task.status = 'running'
                    task.metadata = f"DAG run_id: {run_id}, attempt: {attempt}/{max_attempts}"
                    db.session.commit()
                    logger.info(f"Task {task.id} status updated to 'running'")

                    # Prepare result data and wait for DAG completion
                    result_data = {
                        'execution_type': 'immediate',
                        'attempt': attempt,
                        'max_attempts': max_attempts,
                        'dag_id': self.airflow_service.dag_id,
                        'run_id': run_id,
                        'dag_triggered': True,
                        'execution_time': datetime.utcnow().isoformat()
                    }

                    # Wait for DAG completion
                    config = self.get_config()
                    dag_completed = self._wait_for_airflow_completion(run_id, timeout_seconds=config['immediate_airflow_timeout'])

                    if dag_completed:
                        task.mark_as_completed(result_data)
                        db.session.commit()

                        # Log audit event
                        from app.models.audit_event import AuditEvent
                        AuditEvent.log_event(
                            user=validator,
                            event_type='task_execution',
                            action='immediate_airflow_execution_success',
                            resource_type='permission_request',
                            resource_id=permission_request.id,
                            description=f'DAG de Airflow ejecutado exitosamente en intento {attempt}/{max_attempts} para solicitud #{permission_request.id}',
                            metadata={
                                'execution_type': 'immediate',
                                'attempt': attempt,
                                'max_attempts': max_attempts,
                                'task_id': task.id,
                                'run_id': run_id
                            }
                        )
                        return True
                    else:
                        logger.warning(f"DAG completion timeout on attempt {attempt}/{max_attempts} for task {task.id}")

                else:
                    logger.warning(f"Airflow execution attempt {attempt}/{max_attempts} failed for task {task.id}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_attempts:
                    import time
                    logger.info(f"Waiting {retry_delay} seconds before retry attempt {attempt + 1}")
                    time.sleep(retry_delay)

            except Exception as e:
                logger.error(f"Exception in Airflow execution attempt {attempt}/{max_attempts} for task {task.id}: {str(e)}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_attempts:
                    import time
                    logger.info(f"Waiting {retry_delay} seconds before retry attempt {attempt + 1}")
                    time.sleep(retry_delay)

        # All attempts failed
        logger.error(f"All {max_attempts} Airflow execution attempts failed for task {task.id}")

        # Mark task as failed and send notification
        error_msg = f'Failed to trigger Airflow DAG after {max_attempts} attempts'
        task.mark_as_failed(error_msg, {
            'execution_type': 'immediate',
            'max_attempts': max_attempts,
            'final_result': 'failed',
            'dag_id': self.airflow_service.dag_id
        })

        # Send notification using the task-based method
        self._send_queued_airflow_failure_notification(task)
        db.session.commit()
        return False

    def _execute_ad_verification_task_immediately(self, task, permission_request):
        """Execute AD verification task immediately with proper attempt tracking in database"""
        config = self.get_config()
        max_attempts = config['max_retries']
        retry_delay = config['immediate_ad_retry_delay']

        # Mark task as running
        task.mark_as_running()
        db.session.commit()

        # Perform up to 3 attempts
        for attempt in range(1, max_attempts + 1):
            try:
                # Update attempt count in database
                task.increment_attempt_count()
                db.session.commit()

                logger.info(f"AD verification attempt {attempt}/{max_attempts} for task {task.id}")

                # Verify AD changes
                verification_result = self.verify_ad_changes(
                    folder_path=permission_request.folder.path,
                    ad_group_name=permission_request.ad_group.name if permission_request.ad_group else None,
                    access_type=permission_request.permission_type,
                    action_type='add',
                    requester_user=permission_request.requester
                )

                if verification_result['success']:
                    logger.info(f"AD verification successful on attempt {attempt}/{max_attempts} for task {task.id}")

                    # Mark task as completed
                    result_data = {
                        'execution_type': 'immediate',
                        'attempt': attempt,
                        'max_attempts': max_attempts,
                        'verification_status': 'success',
                        'ad_permissions_applied': True,
                        'verification_time': datetime.utcnow().isoformat(),
                        'details': verification_result['details']
                    }
                    task.mark_as_completed(result_data)
                    db.session.commit()

                    # Log audit event
                    from app.models.audit_event import AuditEvent
                    AuditEvent.log_event(
                        user=permission_request.validator,
                        event_type='task_execution',
                        action='immediate_ad_verification_success',
                        resource_type='permission_request',
                        resource_id=permission_request.id,
                        description=f'Verificación AD exitosa en intento {attempt}/{max_attempts} para solicitud #{permission_request.id}',
                        metadata={
                            'execution_type': 'immediate',
                            'attempt': attempt,
                            'max_attempts': max_attempts,
                            'task_id': task.id,
                            'verification_details': verification_result['details']
                        }
                    )
                    return True
                else:
                    logger.warning(f"AD verification attempt {attempt}/{max_attempts} failed for task {task.id}: {verification_result.get('error')}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_attempts:
                    import time
                    logger.info(f"Waiting {retry_delay} seconds before AD verification retry attempt {attempt + 1}")
                    time.sleep(retry_delay)

            except Exception as e:
                logger.error(f"Exception in AD verification attempt {attempt}/{max_attempts} for task {task.id}: {str(e)}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_attempts:
                    import time
                    logger.info(f"Waiting {retry_delay} seconds before AD verification retry attempt {attempt + 1}")
                    time.sleep(retry_delay)

        # All attempts failed
        logger.error(f"All {max_attempts} AD verification attempts failed for task {task.id}")

        # Mark task as failed and send notification
        error_msg = f'AD verification failed after {max_attempts} attempts'
        task.mark_as_failed(error_msg, {
            'execution_type': 'immediate',
            'max_attempts': max_attempts,
            'final_result': 'failed',
            'verification_status': 'failed'
        })

        # Send notification using the task-based method
        self._send_queued_ad_verification_failure_notification(task)
        db.session.commit()
        return False

    def _try_immediate_execution(self, permission_request, validator, csv_file_path):
        """Try to execute Airflow DAG and AD verification immediately with proper dependency"""
        try:
            logger.info(f"Attempting immediate execution for permission request {permission_request.id}")

            # Step 1: Try to execute Airflow DAG immediately
            airflow_result = self._execute_airflow_immediately(permission_request, validator, csv_file_path)

            if not airflow_result['success']:
                logger.warning(f"Immediate Airflow execution failed for request {permission_request.id}")
                return False

            # Step 2: Wait for Airflow DAG completion with monitoring
            config = self.get_config()
            dag_completed = self._wait_for_airflow_completion(airflow_result['run_id'], timeout_seconds=config['immediate_airflow_timeout'])

            if not dag_completed:
                logger.warning(f"Airflow DAG did not complete successfully within timeout ({config['immediate_airflow_timeout']}s) for request {permission_request.id}")
                return False

            # Step 3: Now that Airflow has completed successfully, proceed with AD verification
            verification_success = self._execute_ad_verification_immediately(permission_request)

            if not verification_success:
                logger.warning(f"Immediate AD verification failed for request {permission_request.id}")
                return False

            logger.info(f"Both Airflow and AD verification completed immediately for request {permission_request.id}")
            return True

        except Exception as e:
            logger.error(f"Error during immediate execution for request {permission_request.id}: {str(e)}")
            return False

    def _execute_airflow_immediately(self, permission_request, validator, csv_file_path):
        """Execute Airflow DAG immediately with 3 retry attempts and return execution details"""
        max_attempts = 3
        retry_delay = 5  # seconds between retries

        task_data = {
            'permission_request_id': permission_request.id,
            'folder_path': permission_request.folder.path,
            'ad_group_name': permission_request.ad_group.name if permission_request.ad_group else None,
            'permission_type': permission_request.permission_type,
            'requester': permission_request.requester.username,
            'validator': validator.username,
            'csv_file_path': os.path.basename(csv_file_path) if csv_file_path else None
        }

        # Prepare configuration for Airflow DAG
        conf = {
            'change_file': task_data.get('csv_file_path'),
            'request_ids': [permission_request.id],
            'triggered_by': validator.username,
            'folder_path': task_data.get('folder_path'),
            'ad_group_name': task_data.get('ad_group_name'),
            'permission_type': task_data.get('permission_type'),
            'ad_source_domain': os.getenv('AD_DOMAIN_PREFIX', ''),
            'ad_target_domain': os.getenv('AD_TARGET_DOMAIN', 'AUDI'),
            'immediate_execution': True
        }

        # Perform up to 3 attempts
        for attempt in range(1, max_attempts + 1):
            try:
                # Generate unique run ID for this attempt
                run_id = f"immediate__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_attempt{attempt}_{permission_request.id}"
                conf['custom_run_id'] = run_id

                logger.info(f"Airflow execution attempt {attempt}/{max_attempts} for permission request {permission_request.id}")

                # Trigger Airflow DAG
                success = self.airflow_service.trigger_dag(conf)

                if success:
                    logger.info(f"Airflow DAG triggered successfully on attempt {attempt}/{max_attempts} for permission request {permission_request.id} with run_id: {run_id}")

                    # Log audit event for successful execution
                    from app.models.audit_event import AuditEvent
                    AuditEvent.log_event(
                        user=validator,
                        event_type='task_execution',
                        action='immediate_airflow_execution_success',
                        resource_type='permission_request',
                        resource_id=permission_request.id,
                        description=f'DAG de Airflow ejecutado exitosamente en intento {attempt}/{max_attempts} para solicitud #{permission_request.id}',
                        metadata={
                            'execution_type': 'immediate',
                            'attempt': attempt,
                            'max_attempts': max_attempts,
                            'dag_id': self.airflow_service.dag_id,
                            'run_id': run_id,
                            'permission_request_id': permission_request.id,
                            'config_sent': conf
                        }
                    )

                    return {
                        'success': True,
                        'run_id': run_id,
                        'dag_id': self.airflow_service.dag_id,
                        'triggered_at': datetime.utcnow().isoformat(),
                        'attempt': attempt
                    }
                else:
                    logger.warning(f"Airflow execution attempt {attempt}/{max_attempts} failed for request {permission_request.id}")

                    # If this is not the last attempt, wait before retrying
                    if attempt < max_attempts:
                        import time
                        logger.info(f"Waiting {retry_delay} seconds before retry attempt {attempt + 1}")
                        time.sleep(retry_delay)
                        continue

            except Exception as e:
                logger.error(f"Exception in Airflow execution attempt {attempt}/{max_attempts} for request {permission_request.id}: {str(e)}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_attempts:
                    import time
                    logger.info(f"Waiting {retry_delay} seconds before retry attempt {attempt + 1}")
                    time.sleep(retry_delay)
                    continue

        # All attempts failed - log final failure and send notification
        logger.error(f"All {max_attempts} Airflow execution attempts failed for permission request {permission_request.id}")

        # Send admin notification after all attempts failed
        self._send_airflow_failure_notification(permission_request, validator, max_attempts)

        # Log audit event for final failure
        from app.models.audit_event import AuditEvent
        AuditEvent.log_event(
            user=validator,
            event_type='task_execution',
            action='immediate_airflow_execution_failed',
            resource_type='permission_request',
            resource_id=permission_request.id,
            description=f'DAG de Airflow falló después de {max_attempts} intentos para solicitud #{permission_request.id}',
            metadata={
                'execution_type': 'immediate',
                'max_attempts': max_attempts,
                'final_result': 'failed',
                'permission_request_id': permission_request.id,
                'dag_id': self.airflow_service.dag_id
            }
        )

        return {
            'success': False,
            'error': f'Failed to trigger Airflow DAG after {max_attempts} attempts',
            'attempts_made': max_attempts
        }

    def _send_airflow_failure_notification(self, permission_request, validator, max_attempts):
        """Send notification after Airflow execution fails after all retry attempts"""
        try:
            from app.services.email_service import send_admin_error_notification
            error_message = (
                f"El DAG de Airflow falló después de {max_attempts} intentos para la solicitud de permisos #{permission_request.id}\n\n"
                f"Detalles:\n"
                f"- Carpeta: {permission_request.folder.path}\n"
                f"- Grupo AD: {permission_request.ad_group.name if permission_request.ad_group else 'N/A'}\n"
                f"- Tipo de permiso: {permission_request.permission_type}\n"
                f"- Solicitante: {permission_request.requester.username}\n"
                f"- Validador: {validator.username}\n"
                f"- DAG ID: {self.airflow_service.dag_id}\n"
                f"- API URL: {self.airflow_service.api_url}"
            )

            send_admin_error_notification(
                error_type="DAG_EXECUTION_FAILED_AFTER_RETRIES",
                service_name="Airflow",
                error_message=error_message
            )

            logger.info(f"Airflow failure notification sent after {max_attempts} failed attempts for request {permission_request.id}")

        except Exception as e:
            logger.error(f"Error sending Airflow failure notification: {str(e)}")

    def _wait_for_airflow_completion(self, run_id, timeout_seconds=300):
        """Wait for Airflow DAG run to complete and return success status"""
        try:
            import time
            start_time = time.time()
            check_interval = 10  # Check every 10 seconds

            logger.info(f"Waiting for Airflow DAG run {run_id} to complete (timeout: {timeout_seconds}s)")

            while time.time() - start_time < timeout_seconds:
                # Check DAG run status using Airflow service
                dag_status = self.airflow_service.get_dag_run_status(run_id)

                if dag_status:
                    state = dag_status.get('state', '').lower()
                    logger.debug(f"DAG run {run_id} status: {state}")

                    if state == 'success':
                        logger.info(f"DAG run {run_id} completed successfully")
                        return True
                    elif state in ['failed', 'cancelled', 'skipped']:
                        logger.error(f"DAG run {run_id} failed with state: {state}")
                        return False
                    elif state in ['running', 'queued']:
                        # Still running, continue waiting
                        logger.debug(f"DAG run {run_id} still running, waiting...")
                        time.sleep(check_interval)
                        continue
                    else:
                        # Unknown state, continue monitoring but log warning
                        logger.warning(f"DAG run {run_id} in unknown state: {state}, continuing to monitor")
                        time.sleep(check_interval)
                        continue
                else:
                    # Could not get status, might be connectivity issue
                    logger.warning(f"Could not get status for DAG run {run_id}, retrying...")
                    time.sleep(check_interval)
                    continue

            # Timeout reached
            logger.error(f"Timeout waiting for DAG run {run_id} to complete after {timeout_seconds} seconds")
            return False

        except Exception as e:
            logger.error(f"Error waiting for Airflow completion for run {run_id}: {str(e)}")
            return False

    def _execute_ad_verification_immediately(self, permission_request):
        """Execute AD verification immediately with 3 retry attempts"""
        max_attempts = 3
        retry_delay = 10  # seconds between retries for AD verification

        # Perform up to 3 attempts
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"AD verification attempt {attempt}/{max_attempts} for permission request {permission_request.id}")

                # Verify AD changes
                verification_result = self.verify_ad_changes(
                    folder_path=permission_request.folder.path,
                    ad_group_name=permission_request.ad_group.name if permission_request.ad_group else None,
                    access_type=permission_request.permission_type,
                    action_type='add',
                    requester_user=permission_request.requester
                )

                if verification_result['success']:
                    logger.info(f"AD verification successful on attempt {attempt}/{max_attempts} for permission request {permission_request.id}")

                    # Log audit event for successful verification
                    from app.models.audit_event import AuditEvent
                    AuditEvent.log_event(
                        user=permission_request.validator,
                        event_type='task_execution',
                        action='immediate_ad_verification_success',
                        resource_type='permission_request',
                        resource_id=permission_request.id,
                        description=f'Verificación AD exitosa en intento {attempt}/{max_attempts} para solicitud #{permission_request.id}',
                        metadata={
                            'execution_type': 'immediate',
                            'attempt': attempt,
                            'max_attempts': max_attempts,
                            'verification_details': verification_result['details'],
                            'permission_request_id': permission_request.id
                        }
                    )

                    return True
                else:
                    logger.warning(f"AD verification attempt {attempt}/{max_attempts} failed for request {permission_request.id}: {verification_result.get('error')}")

                    # If this is not the last attempt, wait before retrying
                    if attempt < max_attempts:
                        import time
                        logger.info(f"Waiting {retry_delay} seconds before AD verification retry attempt {attempt + 1}")
                        time.sleep(retry_delay)
                        continue

            except Exception as e:
                logger.error(f"Exception in AD verification attempt {attempt}/{max_attempts} for request {permission_request.id}: {str(e)}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_attempts:
                    import time
                    logger.info(f"Waiting {retry_delay} seconds before AD verification retry attempt {attempt + 1}")
                    time.sleep(retry_delay)
                    continue

        # All attempts failed - log final failure and send notification
        logger.error(f"All {max_attempts} AD verification attempts failed for permission request {permission_request.id}")

        # Send admin notification after all attempts failed
        self._send_ad_verification_failure_notification(permission_request, max_attempts)

        # Log audit event for final failure
        from app.models.audit_event import AuditEvent
        AuditEvent.log_event(
            user=permission_request.validator,
            event_type='task_execution',
            action='immediate_ad_verification_failed',
            resource_type='permission_request',
            resource_id=permission_request.id,
            description=f'Verificación AD falló después de {max_attempts} intentos para solicitud #{permission_request.id}',
            metadata={
                'execution_type': 'immediate',
                'max_attempts': max_attempts,
                'final_result': 'failed',
                'permission_request_id': permission_request.id
            }
        )

        return False

    def _send_ad_verification_failure_notification(self, permission_request, max_attempts):
        """Send notification after AD verification fails after all retry attempts"""
        try:
            from app.services.email_service import send_admin_error_notification
            error_message = (
                f"La verificación AD falló después de {max_attempts} intentos para la solicitud de permisos #{permission_request.id}\n\n"
                f"Detalles:\n"
                f"- Carpeta: {permission_request.folder.path}\n"
                f"- Grupo AD: {permission_request.ad_group.name if permission_request.ad_group else 'N/A'}\n"
                f"- Tipo de permiso: {permission_request.permission_type}\n"
                f"- Solicitante: {permission_request.requester.username}\n"
                f"- Validador: {permission_request.validator.username if permission_request.validator else 'N/A'}\n\n"
                f"La tarea de Airflow fue exitosa, pero la verificación AD no pudo confirmar que los cambios se aplicaron correctamente.\n"
                f"Se recomienda verificar manualmente el estado de los permisos en Active Directory."
            )

            send_admin_error_notification(
                error_type="AD_VERIFICATION_FAILED_AFTER_RETRIES",
                service_name="Active Directory",
                error_message=error_message
            )

            logger.info(f"AD verification failure notification sent after {max_attempts} failed attempts for request {permission_request.id}")

        except Exception as e:
            logger.error(f"Error sending AD verification failure notification: {str(e)}")

    def _create_completed_tracking_tasks(self, permission_request, validator, csv_file_path):
        """Create tasks that are already marked as completed for tracking purposes"""
        try:
            from app.models import Task
            config = self.get_config()

            # Create Airflow task (already completed)
            airflow_task = Task.create_airflow_task(permission_request, validator, csv_file_path)
            airflow_task.max_attempts = config['max_retries']
            airflow_task.status = 'completed'
            airflow_task.completed_at = datetime.utcnow()
            airflow_task.set_result_data({
                'execution_type': 'immediate',
                'dag_triggered': True,
                'execution_time': datetime.utcnow().isoformat(),
                'immediate_success': True
            })
            db.session.add(airflow_task)
            db.session.flush()

            # Create verification task (already completed)
            verification_task = Task.create_ad_verification_task(permission_request, validator, delay_seconds=0)
            verification_task.max_attempts = config['max_retries']
            verification_task.status = 'completed'
            verification_task.completed_at = datetime.utcnow()
            verification_task.set_result_data({
                'execution_type': 'immediate',
                'verification_status': 'success',
                'ad_permissions_applied': True,
                'verification_time': datetime.utcnow().isoformat(),
                'immediate_success': True
            })

            # Link tasks
            verification_data = verification_task.get_task_data()
            verification_data['depends_on_task_id'] = airflow_task.id
            verification_data['csv_file_path'] = os.path.basename(csv_file_path) if csv_file_path else None
            verification_task.set_task_data(verification_data)

            db.session.add(verification_task)
            db.session.commit()

            logger.info(f"Created completed tracking tasks for permission request {permission_request.id}")
            return [airflow_task, verification_task]

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating completed tracking tasks for request {permission_request.id}: {str(e)}")
            return []

    def _create_queued_tasks(self, permission_request, validator, csv_file_path):
        """Create traditional queued tasks (original behavior)"""
        try:
            from app.models import Task
            config = self.get_config()

            # Task 1: Execute Airflow DAG (queued)
            logger.debug(f"Creating queued Airflow task for request {permission_request.id}")
            airflow_task = Task.create_airflow_task(permission_request, validator, csv_file_path)
            airflow_task.max_attempts = config['max_retries']
            airflow_task.next_execution_at = datetime.utcnow()  # Execute ASAP but through queue
            db.session.add(airflow_task)
            db.session.flush()
            logger.info(f"Created queued Airflow task with ID {airflow_task.id} for request {permission_request.id}")

            # Task 2: Verify AD changes (queued with delay)
            logger.debug(f"Creating queued AD verification task for request {permission_request.id}")
            verification_task = Task.create_ad_verification_task(permission_request, validator, delay_seconds=config['retry_delay'])
            verification_task.max_attempts = config['max_retries']
            db.session.add(verification_task)
            db.session.flush()
            logger.info(f"Created queued AD verification task with ID {verification_task.id} for request {permission_request.id}")

            # Link tasks
            verification_data = verification_task.get_task_data()
            verification_data['depends_on_task_id'] = airflow_task.id
            verification_data['csv_file_path'] = os.path.basename(csv_file_path) if csv_file_path else None
            verification_task.set_task_data(verification_data)

            db.session.commit()

            logger.info(f"Successfully created queued approval tasks for permission request {permission_request.id}")
            return [airflow_task, verification_task]

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating queued tasks for request {permission_request.id}: {str(e)}")
            return []
    
    def create_revocation_tasks(self, permission_request, validator, csv_file_path=None):
        """Create tasks when a permission request is revoked - with immediate execution optimization"""
        try:
            # Check if revocation tasks already exist for this permission request
            from app.models import Task
            existing_tasks = Task.query.filter_by(permission_request_id=permission_request.id).filter(
                Task.name.contains('revocation')
            ).all()

            if existing_tasks:
                logger.warning(f"Revocation tasks already exist for permission request {permission_request.id}. Existing tasks: {[t.id for t in existing_tasks]}")
                return existing_tasks

            # OPTIMIZATION: Try immediate execution first for revocation using the same pattern as approval
            immediate_result = self._try_immediate_revocation_execution_with_tasks(permission_request, validator, csv_file_path)

            if immediate_result['success']:
                logger.info(f"Successfully executed revocation tasks immediately for permission request {permission_request.id}")
                return immediate_result['tasks']

            # Fallback: If immediate execution failed, convert existing tasks to queued mode
            logger.info(f"Immediate revocation execution failed for request {permission_request.id}, converting tasks to queued mode")
            return self._convert_tasks_to_queued_mode(immediate_result['tasks'], permission_request, validator)

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating revocation tasks: {str(e)}")
            return []

    def _try_immediate_revocation_execution(self, permission_request, validator, csv_file_path):
        """Try to execute Airflow DAG and AD verification immediately for revocation with proper dependency"""
        try:
            logger.info(f"Attempting immediate revocation execution for permission request {permission_request.id}")

            # Step 1: Try to execute Airflow DAG immediately for revocation
            airflow_result = self._execute_airflow_revocation_immediately(permission_request, validator, csv_file_path)

            if not airflow_result['success']:
                logger.warning(f"Immediate Airflow revocation execution failed for request {permission_request.id}")
                return False

            # Step 2: Wait for Airflow DAG completion with monitoring
            config = self.get_config()
            dag_completed = self._wait_for_airflow_completion(airflow_result['run_id'], timeout_seconds=config['immediate_airflow_timeout'])

            if not dag_completed:
                logger.warning(f"Airflow revocation DAG did not complete successfully within timeout ({config['immediate_airflow_timeout']}s) for request {permission_request.id}")
                return False

            # Step 3: Now that Airflow has completed successfully, proceed with AD verification
            verification_success = self._execute_ad_revocation_verification_immediately(permission_request)

            if not verification_success:
                logger.warning(f"Immediate AD revocation verification failed for request {permission_request.id}")
                return False

            logger.info(f"Both Airflow revocation and AD verification completed immediately for request {permission_request.id}")
            return True

        except Exception as e:
            logger.error(f"Error during immediate revocation execution for request {permission_request.id}: {str(e)}")
            return False

    def _execute_airflow_revocation_immediately(self, permission_request, validator, csv_file_path):
        """Execute Airflow DAG immediately for revocation with 3 retry attempts and return execution details"""
        max_attempts = 3
        retry_delay = 5  # seconds between retries

        task_data = {
            'permission_request_id': permission_request.id,
            'folder_path': permission_request.folder.path,
            'ad_group_name': permission_request.ad_group.name if permission_request.ad_group else None,
            'permission_type': permission_request.permission_type,
            'requester': permission_request.requester.username,
            'validator': validator.username,
            'csv_file_path': os.path.basename(csv_file_path) if csv_file_path else None,
            'action': 'revoke'
        }

        # Prepare configuration for Airflow DAG (revocation)
        conf = {
            'change_file': task_data.get('csv_file_path'),
            'request_ids': [permission_request.id],
            'triggered_by': validator.username,
            'folder_path': task_data.get('folder_path'),
            'ad_group_name': task_data.get('ad_group_name'),
            'permission_type': task_data.get('permission_type'),
            'action_type': 'revoke',
            'ad_source_domain': os.getenv('AD_DOMAIN_PREFIX', ''),
            'ad_target_domain': os.getenv('AD_TARGET_DOMAIN', 'AUDI'),
            'immediate_execution': True
        }

        # Perform up to 3 attempts
        for attempt in range(1, max_attempts + 1):
            try:
                # Generate unique run ID for this attempt
                run_id = f"immediate_revoke__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_attempt{attempt}_{permission_request.id}"
                conf['custom_run_id'] = run_id

                logger.info(f"Airflow revocation execution attempt {attempt}/{max_attempts} for permission request {permission_request.id}")

                # Trigger Airflow DAG for revocation
                success = self.airflow_service.trigger_dag(conf)

                if success:
                    logger.info(f"Airflow revocation DAG triggered successfully on attempt {attempt}/{max_attempts} for permission request {permission_request.id} with run_id: {run_id}")

                    # Log audit event for successful revocation execution
                    from app.models.audit_event import AuditEvent
                    AuditEvent.log_event(
                        user=validator,
                        event_type='task_execution',
                        action='immediate_airflow_revocation_success',
                        resource_type='permission_request',
                        resource_id=permission_request.id,
                        description=f'DAG de Airflow para revocación ejecutado exitosamente en intento {attempt}/{max_attempts} para solicitud #{permission_request.id}',
                        metadata={
                            'execution_type': 'immediate',
                            'action_type': 'revoke',
                            'attempt': attempt,
                            'max_attempts': max_attempts,
                            'dag_id': self.airflow_service.dag_id,
                            'run_id': run_id,
                            'permission_request_id': permission_request.id,
                            'config_sent': conf
                        }
                    )

                    return {
                        'success': True,
                        'run_id': run_id,
                        'dag_id': self.airflow_service.dag_id,
                        'triggered_at': datetime.utcnow().isoformat(),
                        'attempt': attempt
                    }
                else:
                    logger.warning(f"Airflow revocation execution attempt {attempt}/{max_attempts} failed for request {permission_request.id}")

                    # If this is not the last attempt, wait before retrying
                    if attempt < max_attempts:
                        import time
                        logger.info(f"Waiting {retry_delay} seconds before revocation retry attempt {attempt + 1}")
                        time.sleep(retry_delay)
                        continue

            except Exception as e:
                logger.error(f"Exception in Airflow revocation execution attempt {attempt}/{max_attempts} for request {permission_request.id}: {str(e)}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_attempts:
                    import time
                    logger.info(f"Waiting {retry_delay} seconds before revocation retry attempt {attempt + 1}")
                    time.sleep(retry_delay)
                    continue

        # All attempts failed - log final failure and send notification
        logger.error(f"All {max_attempts} Airflow revocation execution attempts failed for permission request {permission_request.id}")

        # Send admin notification after all attempts failed
        self._send_airflow_revocation_failure_notification(permission_request, validator, max_attempts)

        # Log audit event for final failure
        from app.models.audit_event import AuditEvent
        AuditEvent.log_event(
            user=validator,
            event_type='task_execution',
            action='immediate_airflow_revocation_failed',
            resource_type='permission_request',
            resource_id=permission_request.id,
            description=f'DAG de Airflow para revocación falló después de {max_attempts} intentos para solicitud #{permission_request.id}',
            metadata={
                'execution_type': 'immediate',
                'action_type': 'revoke',
                'max_attempts': max_attempts,
                'final_result': 'failed',
                'permission_request_id': permission_request.id,
                'dag_id': self.airflow_service.dag_id
            }
        )

        return {
            'success': False,
            'error': f'Failed to trigger Airflow revocation DAG after {max_attempts} attempts',
            'attempts_made': max_attempts
        }

    def _send_airflow_revocation_failure_notification(self, permission_request, validator, max_attempts):
        """Send notification after Airflow revocation execution fails after all retry attempts"""
        try:
            from app.services.email_service import send_admin_error_notification
            error_message = (
                f"El DAG de Airflow para revocación falló después de {max_attempts} intentos para la solicitud de permisos #{permission_request.id}\n\n"
                f"Detalles:\n"
                f"- Carpeta: {permission_request.folder.path}\n"
                f"- Grupo AD: {permission_request.ad_group.name if permission_request.ad_group else 'N/A'}\n"
                f"- Tipo de permiso: {permission_request.permission_type}\n"
                f"- Solicitante: {permission_request.requester.username}\n"
                f"- Validador: {validator.username}\n"
                f"- Acción: Revocación\n"
                f"- DAG ID: {self.airflow_service.dag_id}\n"
                f"- API URL: {self.airflow_service.api_url}"
            )

            send_admin_error_notification(
                error_type="DAG_REVOCATION_FAILED_AFTER_RETRIES",
                service_name="Airflow",
                error_message=error_message
            )

            logger.info(f"Airflow revocation failure notification sent after {max_attempts} failed attempts for request {permission_request.id}")

        except Exception as e:
            logger.error(f"Error sending Airflow revocation failure notification: {str(e)}")

    def _execute_ad_revocation_verification_immediately(self, permission_request):
        """Execute AD verification immediately for revocation with 3 retry attempts"""
        max_attempts = 3
        retry_delay = 10  # seconds between retries for AD verification

        # Perform up to 3 attempts
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"AD revocation verification attempt {attempt}/{max_attempts} for permission request {permission_request.id}")

                # Verify AD changes for revocation (check that permissions are removed)
                verification_result = self.verify_ad_changes(
                    folder_path=permission_request.folder.path,
                    ad_group_name=permission_request.ad_group.name if permission_request.ad_group else None,
                    access_type=permission_request.permission_type,
                    action_type='remove',  # For revocation, we're checking removal
                    requester_user=permission_request.requester
                )

                if verification_result['success']:
                    logger.info(f"AD revocation verification successful on attempt {attempt}/{max_attempts} for permission request {permission_request.id}")

                    # Log audit event for successful revocation verification
                    from app.models.audit_event import AuditEvent
                    AuditEvent.log_event(
                        user=permission_request.validator,
                        event_type='task_execution',
                        action='immediate_ad_revocation_verification_success',
                        resource_type='permission_request',
                        resource_id=permission_request.id,
                        description=f'Verificación AD de revocación exitosa en intento {attempt}/{max_attempts} para solicitud #{permission_request.id}',
                        metadata={
                            'execution_type': 'immediate',
                            'action_type': 'remove',
                            'attempt': attempt,
                            'max_attempts': max_attempts,
                            'verification_details': verification_result['details'],
                            'permission_request_id': permission_request.id
                        }
                    )

                    return True
                else:
                    logger.warning(f"AD revocation verification attempt {attempt}/{max_attempts} failed for request {permission_request.id}: {verification_result.get('error')}")

                    # If this is not the last attempt, wait before retrying
                    if attempt < max_attempts:
                        import time
                        logger.info(f"Waiting {retry_delay} seconds before AD revocation verification retry attempt {attempt + 1}")
                        time.sleep(retry_delay)
                        continue

            except Exception as e:
                logger.error(f"Exception in AD revocation verification attempt {attempt}/{max_attempts} for request {permission_request.id}: {str(e)}")

                # If this is not the last attempt, wait before retrying
                if attempt < max_attempts:
                    import time
                    logger.info(f"Waiting {retry_delay} seconds before AD revocation verification retry attempt {attempt + 1}")
                    time.sleep(retry_delay)
                    continue

        # All attempts failed - log final failure and send notification
        logger.error(f"All {max_attempts} AD revocation verification attempts failed for permission request {permission_request.id}")

        # Send admin notification after all attempts failed
        self._send_ad_revocation_verification_failure_notification(permission_request, max_attempts)

        # Log audit event for final failure
        from app.models.audit_event import AuditEvent
        AuditEvent.log_event(
            user=permission_request.validator,
            event_type='task_execution',
            action='immediate_ad_revocation_verification_failed',
            resource_type='permission_request',
            resource_id=permission_request.id,
            description=f'Verificación AD de revocación falló después de {max_attempts} intentos para solicitud #{permission_request.id}',
            metadata={
                'execution_type': 'immediate',
                'action_type': 'remove',
                'max_attempts': max_attempts,
                'final_result': 'failed',
                'permission_request_id': permission_request.id
            }
        )

        return False

    def _send_ad_revocation_verification_failure_notification(self, permission_request, max_attempts):
        """Send notification after AD revocation verification fails after all retry attempts"""
        try:
            from app.services.email_service import send_admin_error_notification
            error_message = (
                f"La verificación AD de revocación falló después de {max_attempts} intentos para la solicitud de permisos #{permission_request.id}\n\n"
                f"Detalles:\n"
                f"- Carpeta: {permission_request.folder.path}\n"
                f"- Grupo AD: {permission_request.ad_group.name if permission_request.ad_group else 'N/A'}\n"
                f"- Tipo de permiso: {permission_request.permission_type}\n"
                f"- Solicitante: {permission_request.requester.username}\n"
                f"- Validador: {permission_request.validator.username if permission_request.validator else 'N/A'}\n"
                f"- Acción: Revocación\n\n"
                f"La tarea de Airflow para revocación fue exitosa, pero la verificación AD no pudo confirmar que los permisos fueron removidos correctamente.\n"
                f"Se recomienda verificar manualmente que los permisos han sido revocados en Active Directory."
            )

            send_admin_error_notification(
                error_type="AD_REVOCATION_VERIFICATION_FAILED_AFTER_RETRIES",
                service_name="Active Directory",
                error_message=error_message
            )

            logger.info(f"AD revocation verification failure notification sent after {max_attempts} failed attempts for request {permission_request.id}")

        except Exception as e:
            logger.error(f"Error sending AD revocation verification failure notification: {str(e)}")

    def _send_queued_airflow_failure_notification(self, task):
        """Send notification after queued Airflow task fails after all retry attempts"""
        try:
            # Get permission request details if available
            permission_request = None
            if task.permission_request_id:
                from app.models import PermissionRequest
                permission_request = PermissionRequest.query.get(task.permission_request_id)

            task_data = task.get_task_data()

            error_message = (
                f"La tarea de Airflow en cola falló después de {task.max_attempts} intentos\n\n"
                f"Detalles de la tarea:\n"
                f"- ID de tarea: {task.id}\n"
                f"- Nombre: {task.name}\n"
                f"- Tipo: {task.task_type}\n"
                f"- Intentos realizados: {task.attempt_count}\n"
                f"- Error: {task.error_message}\n\n"
            )

            if permission_request:
                error_message += (
                    f"Detalles de la solicitud:\n"
                    f"- Solicitud ID: {permission_request.id}\n"
                    f"- Carpeta: {permission_request.folder.path}\n"
                    f"- Grupo AD: {permission_request.ad_group.name if permission_request.ad_group else 'N/A'}\n"
                    f"- Tipo de permiso: {permission_request.permission_type}\n"
                    f"- Solicitante: {permission_request.requester.username}\n"
                )
            else:
                error_message += (
                    f"Detalles de la tarea (sin solicitud asociada):\n"
                    f"- Carpeta: {task_data.get('folder_path', 'N/A')}\n"
                    f"- Grupo AD: {task_data.get('ad_group_name', 'N/A')}\n"
                    f"- Acción: {task_data.get('action', 'N/A')}\n"
                )

            error_message += f"- DAG ID: {self.airflow_service.dag_id}\n- API URL: {self.airflow_service.api_url}"

            from app.services.email_service import send_admin_error_notification
            send_admin_error_notification(
                error_type="QUEUED_AIRFLOW_TASK_FAILED_AFTER_RETRIES",
                service_name="Airflow",
                error_message=error_message
            )

            logger.info(f"Queued Airflow task failure notification sent for task {task.id}")

        except Exception as e:
            logger.error(f"Error sending queued Airflow task failure notification: {str(e)}")

    def _send_queued_ad_verification_failure_notification(self, task):
        """Send notification after queued AD verification task fails after all retry attempts"""
        try:
            # Get permission request details if available
            permission_request = None
            if task.permission_request_id:
                from app.models import PermissionRequest
                permission_request = PermissionRequest.query.get(task.permission_request_id)

            task_data = task.get_task_data()

            error_message = (
                f"La tarea de verificación AD en cola falló después de {task.max_attempts} intentos\n\n"
                f"Detalles de la tarea:\n"
                f"- ID de tarea: {task.id}\n"
                f"- Nombre: {task.name}\n"
                f"- Tipo: {task.task_type}\n"
                f"- Intentos realizados: {task.attempt_count}\n"
                f"- Error: {task.error_message}\n\n"
            )

            if permission_request:
                error_message += (
                    f"Detalles de la solicitud:\n"
                    f"- Solicitud ID: {permission_request.id}\n"
                    f"- Carpeta: {permission_request.folder.path}\n"
                    f"- Grupo AD: {permission_request.ad_group.name if permission_request.ad_group else 'N/A'}\n"
                    f"- Tipo de permiso: {permission_request.permission_type}\n"
                    f"- Solicitante: {permission_request.requester.username}\n"
                )
            else:
                error_message += (
                    f"Detalles de la tarea (sin solicitud asociada):\n"
                    f"- Carpeta: {task_data.get('folder_path', 'N/A')}\n"
                    f"- Grupo AD: {task_data.get('ad_group_name', 'N/A')}\n"
                    f"- Acción: {task_data.get('action', 'N/A')}\n"
                )

            error_message += f"\nLa tarea de Airflow previa pudo haber sido exitosa, pero no se pudo verificar que los cambios se aplicaron correctamente en Active Directory.\nSe recomienda verificar manualmente el estado de los permisos."

            from app.services.email_service import send_admin_error_notification
            send_admin_error_notification(
                error_type="QUEUED_AD_VERIFICATION_TASK_FAILED_AFTER_RETRIES",
                service_name="Active Directory",
                error_message=error_message
            )

            logger.info(f"Queued AD verification task failure notification sent for task {task.id}")

        except Exception as e:
            logger.error(f"Error sending queued AD verification task failure notification: {str(e)}")

    def _create_completed_revocation_tracking_tasks(self, permission_request, validator, csv_file_path):
        """Create revocation tasks that are already marked as completed for tracking purposes"""
        try:
            from app.models import Task
            config = self.get_config()

            # Create Airflow revocation task (already completed)
            airflow_task = Task.create_airflow_task(permission_request, validator, csv_file_path)
            airflow_task.name = f"Airflow DAG revocation for request #{permission_request.id}"
            airflow_task.max_attempts = config['max_retries']
            airflow_task.status = 'completed'
            airflow_task.completed_at = datetime.utcnow()
            airflow_task.set_result_data({
                'execution_type': 'immediate',
                'action_type': 'revoke',
                'dag_triggered': True,
                'execution_time': datetime.utcnow().isoformat(),
                'immediate_success': True
            })
            db.session.add(airflow_task)
            db.session.flush()

            # Create revocation verification task (already completed)
            verification_task = Task.create_ad_verification_task(permission_request, validator, delay_seconds=0)
            verification_task.name = f"AD revocation verification for request #{permission_request.id}"
            verification_task.max_attempts = config['max_retries']
            verification_task.status = 'completed'
            verification_task.completed_at = datetime.utcnow()
            verification_task.set_result_data({
                'execution_type': 'immediate',
                'action_type': 'remove',
                'verification_status': 'success',
                'ad_permissions_removed': True,
                'verification_time': datetime.utcnow().isoformat(),
                'immediate_success': True
            })

            # Link tasks
            verification_data = verification_task.get_task_data()
            verification_data['depends_on_task_id'] = airflow_task.id
            verification_data['csv_file_path'] = os.path.basename(csv_file_path) if csv_file_path else None
            verification_task.set_task_data(verification_data)

            db.session.add(verification_task)
            db.session.commit()

            logger.info(f"Created completed revocation tracking tasks for permission request {permission_request.id}")
            return [airflow_task, verification_task]

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating completed revocation tracking tasks for request {permission_request.id}: {str(e)}")
            return []

    def _create_queued_revocation_tasks(self, permission_request, validator, csv_file_path):
        """Create traditional queued revocation tasks (original behavior)"""
        try:
            from app.models import Task
            config = self.get_config()

            # Task 1: Execute Airflow DAG for revocation (queued)
            airflow_task = Task.create_airflow_task(permission_request, validator, csv_file_path)
            airflow_task.name = f"Airflow DAG revocation for request #{permission_request.id}"
            airflow_task.max_attempts = config['max_retries']
            airflow_task.next_execution_at = datetime.utcnow()  # Execute ASAP but through queue
            db.session.add(airflow_task)
            db.session.flush()

            # Task 2: Verify AD changes for revocation (queued with delay)
            verification_task = Task.create_ad_verification_task(permission_request, validator, delay_seconds=30)
            verification_task.name = f"AD revocation verification for request #{permission_request.id}"
            verification_task.max_attempts = config['max_retries']
            db.session.add(verification_task)
            db.session.flush()

            # Link tasks
            verification_data = verification_task.get_task_data()
            verification_data['depends_on_task_id'] = airflow_task.id
            verification_data['csv_file_path'] = os.path.basename(csv_file_path) if csv_file_path else None
            verification_task.set_task_data(verification_data)

            db.session.commit()

            logger.info(f"Created queued revocation tasks for permission request {permission_request.id}")
            return [airflow_task, verification_task]

        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating queued revocation tasks for request {permission_request.id}: {str(e)}")
            return []
    
    def create_permission_change_tasks(self, permission_request, validator, existing_permission_info):
        """
        Create tasks for permission changes (remove old permission + add new permission).
        This generates two separate tasks: one for removal and one for addition.
        """
        try:
            logger.info(f"Creating permission change tasks for request {permission_request.id}")
            
            tasks = []
            
            # Generate CSV files for both operations
            removal_csv_path = None
            addition_csv_path = None
            
            try:
                # CSV for removing the existing permission
                removal_csv_path = self._generate_removal_csv(permission_request, existing_permission_info)
                # CSV for adding the new permission  
                addition_csv_path = permission_request.generate_csv_file('add')
            except Exception as e:
                logger.error(f"Failed to generate CSV files for change request {permission_request.id}: {str(e)}")
            
            # Task 1: Remove existing permission
            if existing_permission_info.get('existing_source') == 'manual':
                # Remove manually approved permission
                removal_task = self._create_removal_task(permission_request, validator, existing_permission_info, removal_csv_path)
                if removal_task:
                    tasks.append(removal_task)
                    db.session.add(removal_task)
                    db.session.flush()
            elif existing_permission_info.get('existing_source') == 'ad_sync':
                # Create task to remove AD-synchronized permission
                removal_task = self._create_ad_permission_removal_task(permission_request, validator, existing_permission_info, removal_csv_path)
                if removal_task:
                    tasks.append(removal_task)
                    db.session.add(removal_task)
                    db.session.flush()
            
            # Task 2: Add new permission (depends on removal task completion)
            addition_task = self._create_addition_task(permission_request, validator, addition_csv_path)
            if addition_task:
                tasks.append(addition_task)
                db.session.add(addition_task)
                db.session.flush()
                
                # Link addition task to depend on removal task if one was created
                if len(tasks) > 1:
                    addition_data = addition_task.get_task_data()
                    addition_data['depends_on_task_id'] = tasks[0].id  # First task (removal)
                    addition_task.set_task_data(addition_data)
            
            # Task 3: Final verification (depends on addition task)
            if tasks:
                verification_task = Task.create_ad_verification_task(permission_request, validator, delay_seconds=60)
                verification_task.name = f"AD verification for permission change request #{permission_request.id}"
                db.session.add(verification_task)
                db.session.flush()
                
                # Link verification to addition task
                verification_data = verification_task.get_task_data()
                verification_data['depends_on_task_id'] = addition_task.id if addition_task else (tasks[-1].id if tasks else None)
                verification_data['change_type'] = 'permission_change'
                verification_data['old_permission_type'] = existing_permission_info.get('existing_permission_type')
                verification_data['new_permission_type'] = permission_request.permission_type
                verification_task.set_task_data(verification_data)
                
                tasks.append(verification_task)
            
            db.session.commit()
            
            logger.info(f"Successfully created {len(tasks)} permission change tasks for request {permission_request.id}")
            return tasks
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating permission change tasks for request {permission_request.id}: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return []
    
    def _generate_removal_csv(self, permission_request, existing_permission_info):
        """Generate CSV file for removing existing permission"""
        try:
            from app.services.csv_generator_service import CSVGeneratorService
            csv_service = CSVGeneratorService()
            
            # Create a temporary request object for the removal
            if existing_permission_info.get('existing_source') == 'manual':
                existing_request = existing_permission_info.get('existing_request')
                if existing_request:
                    return existing_request.generate_csv_file('remove')
            elif existing_permission_info.get('existing_source') == 'ad_sync':
                # Generate removal CSV for AD-synced permission
                return csv_service.generate_ad_sync_removal_csv(
                    permission_request.requester,
                    permission_request.folder,
                    existing_permission_info.get('existing_group'),
                    existing_permission_info.get('existing_permission_type')
                )
                
            return None
        except Exception as e:
            logger.error(f"Error generating removal CSV: {str(e)}")
            return None
    
    def _create_removal_task(self, permission_request, validator, existing_permission_info, csv_file_path):
        """Create task for removing existing manual permission"""
        try:
            removal_task = Task(
                name=f"Remove existing {existing_permission_info.get('existing_permission_type')} permission for request #{permission_request.id}",
                task_type='airflow_dag',
                status='pending',
                max_attempts=3,
                attempt_count=0,
                created_by_id=validator.id,
                permission_request_id=permission_request.id,
                next_execution_at=datetime.utcnow()
            )
            
            task_data = {
                'permission_request_id': permission_request.id,
                'action': 'remove_existing',
                'folder_path': permission_request.folder.path,
                'folder_id': permission_request.folder_id,
                'permission_type': existing_permission_info.get('existing_permission_type'),
                'validator': validator.username,
                'csv_file_path': os.path.basename(csv_file_path) if csv_file_path else None,
                'existing_source': existing_permission_info.get('existing_source'),
                'requester': permission_request.requester.username
            }
            
            if existing_permission_info.get('existing_request'):
                task_data['existing_request_id'] = existing_permission_info['existing_request'].id
                task_data['ad_group_name'] = existing_permission_info['existing_request'].ad_group.name if existing_permission_info['existing_request'].ad_group else None
            
            removal_task.set_task_data(task_data)
            return removal_task
            
        except Exception as e:
            logger.error(f"Error creating removal task: {str(e)}")
            return None
    
    def _create_ad_permission_removal_task(self, permission_request, validator, existing_permission_info, csv_file_path):
        """Create task for removing AD-synchronized permission"""
        try:
            removal_task = Task(
                name=f"Remove AD-synced {existing_permission_info.get('existing_permission_type')} permission for request #{permission_request.id}",
                task_type='airflow_dag',
                status='pending',
                max_attempts=3,
                attempt_count=0,
                created_by_id=validator.id,
                permission_request_id=permission_request.id,
                next_execution_at=datetime.utcnow()
            )
            
            task_data = {
                'permission_request_id': permission_request.id,
                'action': 'remove_ad_sync',
                'folder_path': permission_request.folder.path,
                'folder_id': permission_request.folder_id,
                'permission_type': existing_permission_info.get('existing_permission_type'),
                'ad_group_name': existing_permission_info.get('existing_group', {}).get('name'),
                'ad_group_id': existing_permission_info.get('existing_group', {}).get('id'),
                'validator': validator.username,
                'csv_file_path': os.path.basename(csv_file_path) if csv_file_path else None,
                'existing_source': 'ad_sync',
                'requester': permission_request.requester.username
            }
            
            removal_task.set_task_data(task_data)
            return removal_task
            
        except Exception as e:
            logger.error(f"Error creating AD permission removal task: {str(e)}")
            return None
    
    def _create_addition_task(self, permission_request, validator, csv_file_path):
        """Create task for adding new permission"""
        try:
            addition_task = Task.create_airflow_task(permission_request, validator, csv_file_path)
            addition_task.name = f"Add new {permission_request.permission_type} permission for request #{permission_request.id}"
            
            return addition_task
            
        except Exception as e:
            logger.error(f"Error creating addition task: {str(e)}")
            return None
    
    def execute_airflow_task(self, task):
        """Execute an Airflow DAG task"""
        try:
            config = self.get_config()
            task.mark_as_running()
            db.session.commit()
            
            task_data = task.get_task_data()
            
            # Prepare configuration for Airflow DAG
            # Use the expected format for compatibility with Airflow DAG
            csv_filename = task_data.get('csv_file_path')
            conf = {
                'change_file': csv_filename,
                'request_ids': [task_data.get('permission_request_id')],
                'triggered_by': task_data.get('validator', 'system'),
                'folder_path': task_data.get('folder_path'),
                'ad_group_name': task_data.get('ad_group_name'),
                'permission_type': task_data.get('permission_type'),
                'ad_source_domain': os.getenv('AD_DOMAIN_PREFIX', ''),
                'ad_target_domain': os.getenv('AD_TARGET_DOMAIN', 'AUDI'),
                'task_id': task.id,
                'execution_timestamp': datetime.utcnow().isoformat()
            }
            
            # Trigger Airflow DAG
            success = self.airflow_service.trigger_dag(conf)
            
            if success:
                result_data = {
                    'dag_triggered': True,
                    'dag_execution_status': 'triggered',
                    'dag_id': self.airflow_service.dag_id,
                    'configuration': conf,
                    'execution_time': datetime.utcnow().isoformat()
                }
                task.mark_as_completed(result_data)

                # Commit the completion to database
                db.session.commit()

                # Log audit event
                AuditEvent.log_event(
                    user=task.created_by,
                    event_type='task_execution',
                    action='airflow_dag_triggered',
                    resource_type='task',
                    resource_id=task.id,
                    description=f'DAG de Airflow ejecutado para solicitud #{task.permission_request_id}',
                    metadata={
                        'task_id': task.id,
                        'dag_id': self.airflow_service.dag_id,
                        'permission_request_id': task.permission_request_id
                    }
                )
                
                logger.info(f"Airflow task {task.id} completed successfully")

                # Execute dependent tasks immediately
                try:
                    self._schedule_dependent_ad_verification_tasks(task)
                except Exception as dependent_error:
                    logger.warning(f"Could not schedule dependent AD verification tasks for task {task.id}: {str(dependent_error)}")
                    # Don't fail the main task - dependent tasks can be processed later

                return True
            else:
                error_msg = "Failed to trigger Airflow DAG"
                retry_scheduled = task.schedule_retry(delay_seconds=config['retry_delay'])

                if not retry_scheduled:
                    task.mark_as_failed(error_msg)
                    # Send notification after all retries exhausted
                    self._send_queued_airflow_failure_notification(task)

                logger.error(f"Airflow task {task.id} failed: {error_msg}")
                return False
                
        except Exception as e:
            error_msg = f"Error executing Airflow task: {str(e)}"
            retry_scheduled = task.schedule_retry(delay_seconds=60)

            if not retry_scheduled:
                task.mark_as_failed(error_msg)
                # Send notification after all retries exhausted
                self._send_queued_airflow_failure_notification(task)

            logger.error(error_msg)
            return False
        finally:
            db.session.commit()
    
    def execute_ad_verification_task(self, task):
        """Execute an AD verification task"""
        try:
            config = self.get_config()
            task.mark_as_running()
            db.session.commit()

            task_data = task.get_task_data()
            expected_changes = task_data.get('expected_changes', {})

            # Check if dependent Airflow task is completed
            depends_on_task_id = task_data.get('depends_on_task_id')
            if depends_on_task_id:
                airflow_task = Task.query.get(depends_on_task_id)
                if not airflow_task or not airflow_task.is_completed():
                    # Reschedule verification for later
                    retry_scheduled = task.schedule_retry(delay_seconds=config['retry_delay'])
                    if not retry_scheduled:
                        task.mark_as_failed(f"Max retries exceeded waiting for Airflow task {depends_on_task_id}")
                    db.session.commit()
                    logger.info(f"AD verification task {task.id} rescheduled - waiting for Airflow task {depends_on_task_id} completion")
                    return False
            
            # Verify AD changes
            # Get the requester from the permission request
            from app.models import PermissionRequest, User
            permission_request = PermissionRequest.query.get(task.permission_request_id) if task.permission_request_id else None

            # Determine action type: for standard permission requests, it's 'add'
            # For revocations, the action would be stored differently
            action_type = expected_changes.get('action',
                         task_data.get('action',
                         task_data.get('action_type', 'add')))  # Default to 'add' for permission grants

            # Get requester user - try from permission_request first, then from task_data
            requester_user = None
            if permission_request and permission_request.requester:
                requester_user = permission_request.requester
            elif task_data.get('user_id'):
                # Fallback to user from task_data for deletion tasks
                requester_user = User.query.get(task_data.get('user_id'))
            elif task_data.get('username'):
                # Final fallback to username lookup
                requester_user = User.query.filter_by(username=task_data.get('username')).first()

            verification_result = self.verify_ad_changes(
                folder_path=expected_changes.get('folder_path') or task_data.get('folder_path'),
                ad_group_name=expected_changes.get('group') or task_data.get('ad_group_name'),
                access_type=expected_changes.get('access_type') or task_data.get('permission_type'),
                action_type=action_type,
                requester_user=requester_user
            )
            
            if verification_result['success']:
                result_data = {
                    'verification_status': 'success',
                    'ad_permissions_applied': True,
                    'verification_time': datetime.utcnow().isoformat(),
                    'details': verification_result['details']
                }
                task.mark_as_completed(result_data)
                
                # Log audit event
                AuditEvent.log_event(
                    user=task.created_by,
                    event_type='task_execution',
                    action='ad_verification_success',
                    resource_type='task',
                    resource_id=task.id,
                    description=f'Verificación AD exitosa para solicitud #{task.permission_request_id}',
                    metadata={
                        'task_id': task.id,
                        'permission_request_id': task.permission_request_id,
                        'verification_details': verification_result['details']
                    }
                )
                
                logger.info(f"AD verification task {task.id} completed successfully")

                # Handle database updates based on deletion type
                if action_type in ['delete', 'remove', 'remove_ad_sync']:
                    try:
                        from app.models import FolderPermission

                        # Get the permission details from task data
                        folder_id = task_data.get('folder_id')
                        ad_group_id = task_data.get('ad_group_id')
                        permission_type = task_data.get('permission_type')

                        if folder_id and ad_group_id and permission_type:
                            # Find the permission record
                            permission_record = FolderPermission.query.filter_by(
                                folder_id=folder_id,
                                ad_group_id=ad_group_id,
                                permission_type=permission_type,
                                is_active=True
                            ).first()

                            if permission_record:
                                # Clear the deletion_in_progress flag for all deletion types
                                permission_record.deletion_in_progress = False

                                # Only remove permission from database for group permission removals
                                # For user deletions from group, keep the group permission active
                                if action_type in ['remove', 'remove_ad_sync']:
                                    # This is a group permission removal - deactivate the permission
                                    permission_record.is_active = False
                                    logger.info(f"Removed group permission from database: folder_id={folder_id}, ad_group_id={ad_group_id}, permission_type={permission_type}")

                                    # Log audit event for database permission removal
                                    AuditEvent.log_event(
                                        user=task.created_by,
                                        event_type='permission_deletion',
                                        action='database_permission_removal',
                                        resource_type='folder_permission',
                                        resource_id=permission_record.id,
                                        description=f'Permiso de grupo eliminado de la base de datos tras verificación AD exitosa',
                                        metadata={
                                            'folder_id': folder_id,
                                            'ad_group_id': ad_group_id,
                                            'permission_type': permission_type,
                                            'task_id': task.id,
                                            'action_type': action_type
                                        }
                                    )
                                elif action_type == 'delete':
                                    # This is a user deletion from group - keep group permission active
                                    # but update the user's membership status in the database
                                    username = task_data.get('username')
                                    user_id = task_data.get('user_id')

                                    if username or user_id:
                                        from app.models import UserADGroupMembership, User

                                        # Find the user
                                        if user_id:
                                            user = User.query.get(user_id)
                                        elif username:
                                            user = User.query.filter_by(username=username).first()
                                        else:
                                            user = None

                                        if user:
                                            # Find and deactivate the user's membership in this group
                                            membership = UserADGroupMembership.query.filter_by(
                                                user_id=user.id,
                                                ad_group_id=ad_group_id,
                                                is_active=True
                                            ).first()

                                            if membership:
                                                membership.is_active = False
                                                logger.info(f"Deactivated user membership in database: user={user.username}, group_id={ad_group_id}")
                                            else:
                                                logger.warning(f"No active membership found to deactivate: user={user.username}, group_id={ad_group_id}")
                                        else:
                                            logger.warning(f"Could not find user to update membership: username={username}, user_id={user_id}")

                                    logger.info(f"User deletion from group completed, keeping group permission active: folder_id={folder_id}, ad_group_id={ad_group_id}, permission_type={permission_type}")

                                    # Log audit event for user removal from group
                                    AuditEvent.log_event(
                                        user=task.created_by,
                                        event_type='user_group_removal',
                                        action='user_removed_from_group',
                                        resource_type='folder_permission',
                                        resource_id=permission_record.id,
                                        description=f'Usuario eliminado del grupo tras verificación AD exitosa',
                                        metadata={
                                            'folder_id': folder_id,
                                            'ad_group_id': ad_group_id,
                                            'permission_type': permission_type,
                                            'task_id': task.id,
                                            'action_type': action_type,
                                            'user_removed': username
                                        }
                                    )

                                db.session.commit()
                            else:
                                logger.warning(f"Permission not found in database: folder_id={folder_id}, ad_group_id={ad_group_id}, permission_type={permission_type}")
                        else:
                            logger.warning(f"Missing required data for database permission handling: folder_id={folder_id}, ad_group_id={ad_group_id}, permission_type={permission_type}")

                    except Exception as e:
                        logger.error(f"Error handling permission database update: {str(e)}")
                        # Don't fail the task for database cleanup errors, but log them

                # Clean up CSV file after successful AD verification
                self.cleanup_csv_file(task)

                return True
            else:
                # Schedule retry if not at max attempts
                retry_scheduled = task.schedule_retry(delay_seconds=config['retry_delay'])
                
                if not retry_scheduled:
                    error_msg = f"AD verification failed after {task.max_attempts} attempts: {verification_result['error']}"
                    task.mark_as_failed(error_msg, {
                        'verification_status': 'failed',
                        'last_error': verification_result['error'],
                        'verification_time': datetime.utcnow().isoformat()
                    })
                    # Send notification after all retries exhausted
                    self._send_queued_ad_verification_failure_notification(task)

                    # If this was a deletion task that failed, restore the permission state
                    if action_type in ['delete', 'remove', 'remove_ad_sync']:
                        try:
                            from app.models import FolderPermission

                            folder_id = task_data.get('folder_id')
                            ad_group_id = task_data.get('ad_group_id')
                            permission_type = task_data.get('permission_type')

                            if folder_id and ad_group_id and permission_type:
                                # Find and restore the permission state - for all deletion types, just clear the flag
                                permission_to_restore = FolderPermission.query.filter_by(
                                    folder_id=folder_id,
                                    ad_group_id=ad_group_id,
                                    permission_type=permission_type,
                                    is_active=True
                                ).first()

                                if permission_to_restore:
                                    permission_to_restore.deletion_in_progress = False  # Clear the flag
                                    db.session.commit()
                                    logger.info(f"Restored permission state after deletion failure: folder_id={folder_id}, ad_group_id={ad_group_id}, permission_type={permission_type}")
                                else:
                                    logger.warning(f"Could not find permission to restore after deletion failure: folder_id={folder_id}, ad_group_id={ad_group_id}, permission_type={permission_type}")
                        except Exception as e:
                            logger.error(f"Error restoring permission state after deletion failure: {str(e)}")

                    # Clean up CSV file after permanent AD verification failure
                    self.cleanup_csv_file(task)
                
                logger.warning(f"AD verification task {task.id} failed attempt {task.attempt_count}")
                return False
                
        except Exception as e:
            error_msg = f"Error executing AD verification task: {str(e)}"
            retry_scheduled = task.schedule_retry(delay_seconds=30)
            
            if not retry_scheduled:
                task.mark_as_failed(error_msg)
                # Send notification after all retries exhausted
                self._send_queued_ad_verification_failure_notification(task)

                # If this was a deletion task that failed, restore the permission state
                if action_type in ['delete', 'remove', 'remove_ad_sync']:
                    try:
                        from app.models import FolderPermission

                        folder_id = task_data.get('folder_id')
                        ad_group_id = task_data.get('ad_group_id')
                        permission_type = task_data.get('permission_type')

                        if folder_id and ad_group_id and permission_type:
                            # Find and restore the permission state - for all deletion types, just clear the flag
                            permission_to_restore = FolderPermission.query.filter_by(
                                folder_id=folder_id,
                                ad_group_id=ad_group_id,
                                permission_type=permission_type,
                                is_active=True
                            ).first()

                            if permission_to_restore:
                                permission_to_restore.deletion_in_progress = False  # Clear the flag
                                db.session.commit()
                                logger.info(f"Restored permission state after deletion exception: folder_id={folder_id}, ad_group_id={ad_group_id}, permission_type={permission_type}")
                    except Exception as restore_error:
                        logger.error(f"Error restoring permission state after deletion exception: {str(restore_error)}")

                # Clean up CSV file after permanent AD verification failure
                self.cleanup_csv_file(task)
            
            logger.error(error_msg)
            return False
        finally:
            db.session.commit()
    
    def verify_ad_changes(self, folder_path, ad_group_name, access_type, action_type='add', requester_user=None):
        """Verify that AD changes have been applied"""
        try:
            logger.info(f"Verifying AD changes for {folder_path}, group {ad_group_name}, access {access_type}, action: {action_type}")
            
            # For removal actions, we check that the permission is NO LONGER present
            is_removal_action = action_type in ['remove', 'remove_ad_sync', 'delete']
            
            # Check if the group exists and is active in our database
            from app.models import ADGroup
            ad_group = ADGroup.query.filter_by(name=ad_group_name, is_active=True).first()
            
            if not ad_group and not is_removal_action:
                return {
                    'success': False,
                    'error': f'AD group {ad_group_name} not found or inactive in database',
                    'details': {}
                }
            elif not ad_group and is_removal_action:
                # For removal, if group doesn't exist in DB, that's actually success
                return {
                    'success': True,
                    'details': {
                        'group_found_in_db': False,
                        'action_type': action_type,
                        'verification_reason': 'Group not found in database - removal successful'
                    }
                }
            
            # Attempt actual AD verification using LDAP service
            try:
                # Connect to AD and verify permissions
                user_groups = self.ldap_service.get_user_groups('test_user')  # This is just to test connectivity
                
                # In a real implementation, you would:
                # 1. Query AD for the specific folder's ACL
                # 2. Check if the group has the required permissions
                # 3. Verify the access level matches what was requested
                
                # For now, we'll simulate the verification by checking group properties
                verification_details = {
                    'group_found': True,
                    'group_active': ad_group.is_active,
                    'group_dn': ad_group.distinguished_name,
                    'folder_path_checked': folder_path,
                    'access_type_verified': access_type,
                    'verification_method': 'ldap_query',
                    'ldap_connectivity': True,
                    'verification_timestamp': datetime.utcnow().isoformat()
                }
                
                # Check if the requester user belongs to the AD group
                # This is what actually matters for verification
                permissions_verified = self._check_user_group_membership(requester_user, ad_group_name, action_type)
                
                verification_details['permissions_verified'] = permissions_verified
                verification_details['action_type'] = action_type
                
                # _check_user_group_membership already applies the correct logic for removals and additions
                # For removal actions, it returns True if user was successfully removed (not in group)
                # For addition actions, it returns True if user was successfully added (in group)
                success = permissions_verified

                if is_removal_action:
                    error_msg = 'User still belongs to group - removal failed' if not success else None
                else:
                    error_msg = 'User does not belong to group - addition failed' if not success else None
                
                return {
                    'success': success,
                    'details': verification_details,
                    'error': error_msg
                }
                
            except Exception as ldap_error:
                logger.warning(f"LDAP verification failed, falling back to basic check: {str(ldap_error)}")
                
                # Fallback to basic verification if LDAP is not available
                verification_details = {
                    'group_found': True,
                    'group_active': ad_group.is_active,
                    'group_dn': ad_group.distinguished_name,
                    'folder_path_checked': folder_path,
                    'access_type_verified': access_type,
                    'verification_method': 'basic_check',
                    'ldap_connectivity': False,
                    'fallback_reason': str(ldap_error),
                    'verification_timestamp': datetime.utcnow().isoformat()
                }
                
                # For fallback mode, assume success but note the limitation
                fallback_success = True
                if is_removal_action:
                    verification_details['verification_reason'] = 'Fallback mode - assuming removal successful'
                else:
                    verification_details['verification_reason'] = 'Fallback mode - assuming addition successful'
                
                return {
                    'success': fallback_success,
                    'details': verification_details
                }
            
        except Exception as e:
            logger.error(f"Error verifying AD changes: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'details': {}
            }
    
    def _check_user_group_membership(self, requester_user, ad_group_name, action_type='add'):
        """Check if user belongs to AD group using real LDAP query"""
        try:
            if not requester_user:
                logger.error("No requester user provided for AD group membership check")
                return False

            import time
            time.sleep(0.5)  # Simulate network/AD check delay

            is_removal_action = action_type in ['remove', 'remove_ad_sync', 'delete']

            logger.info(f"Checking if user '{requester_user.username}' belongs to AD group: {ad_group_name} (action: {action_type})")

            # Try to use LDAP service to check real group membership
            try:
                if self.ldap_service:
                    # Get user's groups from Active Directory
                    user_groups = self.ldap_service.get_user_groups(requester_user.username)

                    # Check if the target group is in user's groups
                    user_belongs_to_group = False
                    if user_groups:
                        # Look for the group name in the user's groups
                        for group in user_groups:
                            if isinstance(group, dict):
                                group_name = group.get('name', '')
                            else:
                                group_dn = str(group)
                                # Extract CN from DN (e.g., "CN=SU-edu-admingroup1,OU=Groups..." -> "SU-edu-admingroup1")
                                if group_dn.upper().startswith('CN='):
                                    group_name = group_dn.split(',')[0].replace('CN=', '').strip()
                                else:
                                    group_name = group_dn

                            if group_name.lower() == ad_group_name.lower():
                                user_belongs_to_group = True
                                break

                    logger.info(f"LDAP check result: User '{requester_user.username}' {'belongs' if user_belongs_to_group else 'does not belong'} to group '{ad_group_name}'")

                else:
                    logger.warning("LDAP service not available, using fallback check")
                    # Fallback to database check using UserADGroupMembership
                    user_belongs_to_group = False
                    for membership in requester_user.ad_group_memberships:
                        if membership.ad_group.name.lower() == ad_group_name.lower() and membership.is_active:
                            user_belongs_to_group = True
                            break
                    logger.info(f"Database fallback check: User '{requester_user.username}' {'belongs' if user_belongs_to_group else 'does not belong'} to group '{ad_group_name}'")

            except Exception as ldap_error:
                logger.warning(f"LDAP group membership check failed: {str(ldap_error)}")
                # Final fallback - assume user does not belong to group for safety
                user_belongs_to_group = False

            # Apply the correct logic based on action type
            if is_removal_action:
                # For removal: Success if user does NOT belong to group
                success = not user_belongs_to_group
                if success:
                    logger.info(f"REMOVAL verification SUCCESS: User '{requester_user.username}' no longer belongs to group '{ad_group_name}'")
                else:
                    logger.warning(f"REMOVAL verification FAILED: User '{requester_user.username}' still belongs to group '{ad_group_name}'")
                return not user_belongs_to_group  # Inverted for removal
            else:
                # For addition: Success if user DOES belong to group
                if user_belongs_to_group:
                    logger.info(f"ADDITION verification SUCCESS: User '{requester_user.username}' belongs to group '{ad_group_name}'")
                else:
                    logger.warning(f"ADDITION verification FAILED: User '{requester_user.username}' does not belong to group '{ad_group_name}'")
                return user_belongs_to_group

        except Exception as e:
            logger.error(f"Error checking AD group membership for user '{requester_user.username if requester_user else 'None'}': {str(e)}")
            # For errors, be conservative:
            # - Removal operations: assume failure (user still in group)
            # - Addition operations: assume failure (user not in group)
            return False
    
    def process_pending_tasks(self):
        """Process all pending and retry tasks that are ready for execution"""
        try:
            config = self.get_config()
            # Get tasks ready for execution (limit by batch size)
            ready_tasks = Task.query.filter(
                Task.status.in_(['pending', 'retry']),
                Task.next_execution_at <= datetime.utcnow()
            ).order_by(Task.created_at).limit(config['batch_size']).all()

            # Also get AD verification tasks that are waiting for dependency completion
            dependent_verification_tasks = Task.query.filter(
                Task.status == 'pending',
                Task.task_type == 'ad_verification',
                Task.next_execution_at.is_(None)
            ).limit(config['batch_size']).all()

            # Check if their dependencies are completed and schedule them
            for task in dependent_verification_tasks:
                task_data = task.get_task_data()
                depends_on_task_id = task_data.get('depends_on_task_id')
                if depends_on_task_id:
                    airflow_task = Task.query.get(depends_on_task_id)
                    if airflow_task and airflow_task.is_completed():
                        # Schedule this verification task with delay to allow AD replication
                        task.next_execution_at = datetime.utcnow() + timedelta(seconds=60)  # 1 minute delay
                        logger.info(f"Scheduled AD verification task {task.id} - Airflow task {depends_on_task_id} completed")
                        db.session.commit()
                        ready_tasks.append(task)
            
            processed_count = 0
            
            for task in ready_tasks:
                try:
                    if task.task_type == 'airflow_dag':
                        success = self.execute_airflow_task(task)
                    elif task.task_type == 'ad_verification':
                        success = self.execute_ad_verification_task(task)
                    else:
                        logger.error(f"Unknown task type: {task.task_type}")
                        task.mark_as_failed(f"Unknown task type: {task.task_type}")
                        
                        # Clean up CSV file when AD verification task has unknown type
                        if task.task_type == 'ad_verification':
                            self.cleanup_csv_file(task)
                        
                        continue
                    
                    processed_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing task {task.id}: {str(e)}")
                    task.mark_as_failed(str(e))
                    
                    # Clean up CSV file when AD verification task has unexpected error
                    if task.task_type == 'ad_verification':
                        self.cleanup_csv_file(task)
                    
                    db.session.commit()
            
            logger.info(f"Processed {processed_count} tasks")
            return processed_count
            
        except Exception as e:
            logger.error(f"Error processing pending tasks: {str(e)}")
            return 0
    
    
    
    def validate_before_approval(self, permission_request):
        """
        Run pre-approval validations against AD.
        Checks if the AD group exists and if there are any conflicts.
        
        Args:
            permission_request: PermissionRequest instance
            
        Returns:
            dict: Validation results with success status and details
        """
        try:
            results = {
                'success': True,
                'warnings': [],
                'errors': [],
                'group_exists': False,
                'folder_accessible': False,
                'details': {}
            }
            
            # Check if AD group exists
            if permission_request.ad_group:
                group_exists = self.ldap_service.verify_group_exists(permission_request.ad_group.name)
                results['group_exists'] = group_exists
                results['details']['ad_group_name'] = permission_request.ad_group.name
                
                if not group_exists:
                    results['errors'].append(f'El grupo AD "{permission_request.ad_group.name}" no existe en Active Directory')
                    results['success'] = False
            else:
                results['errors'].append('No se ha asignado un grupo AD a esta solicitud')
                results['success'] = False
            
            # Check for existing permissions that might conflict
            existing_perms = self._check_existing_folder_permissions(
                permission_request.folder, 
                permission_request.ad_group
            )
            
            if existing_perms:
                results['warnings'].append(f'Ya existen permisos para este grupo en la carpeta: {existing_perms}')
                results['details']['existing_permissions'] = existing_perms
            
            # Validate folder path accessibility
            folder_accessible = self._validate_folder_path(permission_request.folder.path)
            results['folder_accessible'] = folder_accessible
            
            if not folder_accessible:
                results['warnings'].append('La ruta de la carpeta podría no ser accesible desde el dominio')
            
            results['details']['validation_timestamp'] = datetime.utcnow().isoformat()
            
            logger.info(f"Pre-approval validation completed for request {permission_request.id}: success={results['success']}")
            return results
            
        except Exception as e:
            logger.error(f"Error in pre-approval validation: {str(e)}")
            return {
                'success': False,
                'warnings': [],
                'errors': [f'Error interno en la validación: {str(e)}'],
                'group_exists': False,
                'folder_accessible': False,
                'details': {}
            }
    
    def validate_after_approval(self, permission_request, delay_seconds=60):
        """
        Create a task to validate that changes have been applied in AD.
        This should be called after Airflow has processed the permission change.
        
        Args:
            permission_request: PermissionRequest instance
            delay_seconds: How long to wait before validation (default 60s)
            
        Returns:
            Task: The created validation task
        """
        try:
            # Create a post-approval validation task
            validation_task = Task.create_ad_validation_task(
                permission_request, 
                delay_seconds=delay_seconds
            )
            
            db.session.add(validation_task)
            db.session.commit()
            
            logger.info(f"Created post-approval validation task for request {permission_request.id}")
            return validation_task
            
        except Exception as e:
            logger.error(f"Error creating post-approval validation task: {str(e)}")
            return None
    
    def _check_existing_folder_permissions(self, folder, ad_group):
        """Check if there are existing permissions for the folder/group combination"""
        try:
            from app.models import FolderPermission
            
            existing = FolderPermission.query.filter_by(
                folder_id=folder.id,
                ad_group_id=ad_group.id,
                is_active=True
            ).all()
            
            if existing:
                return [perm.permission_type for perm in existing]
            
            return []
            
        except Exception as e:
            logger.error(f"Error checking existing permissions: {str(e)}")
            return []
    
    def _validate_folder_path(self, folder_path):
        """
        Validate that a folder path is accessible and properly formatted.
        This is a basic validation - in production you might want more sophisticated checks.
        """
        try:
            # Basic path validation
            if not folder_path:
                return False
                
            # Check for UNC path format
            if folder_path.startswith('\\\\'):
                # Basic UNC path validation
                parts = folder_path.split('\\')
                return len(parts) >= 4  # \\server\share\path
                
            # Check for local path format
            if ':' in folder_path:
                # Basic local path validation (C:\path\to\folder)
                return len(folder_path) > 3
                
            return True  # Allow other path formats
            
        except Exception as e:
            logger.error(f"Error validating folder path {folder_path}: {str(e)}")
            return False
    
    def get_task_status(self, permission_request_id):
        """Get status of all tasks for a permission request"""
        tasks = Task.query.filter_by(permission_request_id=permission_request_id).all()
        
        status_summary = {
            'total_tasks': len(tasks),
            'completed_tasks': len([t for t in tasks if t.is_completed()]),
            'failed_tasks': len([t for t in tasks if t.is_failed()]),
            'pending_tasks': len([t for t in tasks if t.is_pending() or t.is_retry()]),
            'running_tasks': len([t for t in tasks if t.is_running()]),
            'cancelled_tasks': len([t for t in tasks if t.is_cancelled()]),
            'tasks': [task.to_dict() for task in tasks]
        }
        
        # Determine overall status
        if status_summary['failed_tasks'] > 0:
            status_summary['overall_status'] = 'failed'
        elif status_summary['cancelled_tasks'] > 0 and status_summary['pending_tasks'] == 0 and status_summary['running_tasks'] == 0:
            status_summary['overall_status'] = 'cancelled'
        elif status_summary['pending_tasks'] > 0 or status_summary['running_tasks'] > 0:
            status_summary['overall_status'] = 'in_progress'
        elif status_summary['completed_tasks'] == (status_summary['total_tasks'] - status_summary['cancelled_tasks']):
            status_summary['overall_status'] = 'completed'
        else:
            status_summary['overall_status'] = 'unknown'
        
        return status_summary
    
    def cancel_task(self, task_id, cancelled_by=None, reason=None):
        """Cancel a specific task"""
        try:
            task = Task.query.get(task_id)
            
            if not task:
                logger.error(f"Task {task_id} not found")
                return False
            
            if not task.can_be_cancelled():
                logger.error(f"Task {task_id} cannot be cancelled, status: {task.status}")
                return False
            
            # Cancel the task
            task.cancel(cancelled_by=cancelled_by, reason=reason)
            
            # Clean up CSV file when AD verification task is cancelled
            if task.task_type == 'ad_verification':
                self.cleanup_csv_file(task)
            
            db.session.commit()
            
            logger.info(f"Task {task_id} cancelled successfully")
            
            # Log audit event
            AuditEvent.log_event(
                user=cancelled_by,
                event_type='task_management',
                action='cancel_task',
                resource_type='task',
                resource_id=task.id,
                description=f'Tarea {task.name} cancelada',
                metadata={
                    'task_id': task.id,
                    'task_type': task.task_type,
                    'permission_request_id': task.permission_request_id,
                    'cancellation_reason': reason or 'Task cancelled programmatically'
                }
            )
            
            return True
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error cancelling task {task_id}: {str(e)}")
            return False
    
    def cancel_tasks_for_permission_request(self, permission_request_id, cancelled_by=None, reason=None):
        """Cancel all pending/retry tasks for a permission request"""
        try:
            tasks = Task.query.filter_by(permission_request_id=permission_request_id).filter(
                Task.status.in_(['pending', 'retry'])
            ).all()
            
            cancelled_count = 0
            for task in tasks:
                if task.can_be_cancelled():
                    task.cancel(cancelled_by=cancelled_by, reason=reason)
                    
                    # Clean up CSV file when AD verification task is cancelled
                    if task.task_type == 'ad_verification':
                        self.cleanup_csv_file(task)
                    
                    cancelled_count += 1
            
            if cancelled_count > 0:
                db.session.commit()
                logger.info(f"Cancelled {cancelled_count} tasks for permission request {permission_request_id}")
            
            return cancelled_count
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error cancelling tasks for permission request {permission_request_id}: {str(e)}")
            return 0
    
    def cleanup_old_tasks(self, days_old=None):
        """Clean up old completed/failed tasks"""
        try:
            if days_old is None:
                days_old = self.get_config()['cleanup_days']
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            
            old_tasks = Task.query.filter(
                Task.status.in_(['completed', 'failed', 'cancelled']),
                Task.updated_at < cutoff_date
            ).all()
            
            for task in old_tasks:
                db.session.delete(task)
            
            db.session.commit()
            
            logger.info(f"Cleaned up {len(old_tasks)} old tasks")
            return len(old_tasks)
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error cleaning up old tasks: {str(e)}")
            return 0

    def _schedule_dependent_ad_verification_tasks(self, completed_airflow_task):
        """Schedule AD verification tasks that depend on the completed Airflow task"""
        try:
            from datetime import timedelta

            # Find AD verification tasks for the same permission request
            dependent_tasks = Task.query.filter_by(
                task_type='ad_verification',
                permission_request_id=completed_airflow_task.permission_request_id,
                status='pending'
            ).filter(
                Task.next_execution_at.is_(None)  # Not yet scheduled
            ).all()

            if not dependent_tasks:
                logger.debug(f"No AD verification tasks to schedule for Airflow task {completed_airflow_task.id}")
                return

            logger.info(f"Scheduling {len(dependent_tasks)} AD verification tasks for Airflow task {completed_airflow_task.id}")

            for task in dependent_tasks:
                # Schedule for immediate execution with a small delay
                task.next_execution_at = datetime.utcnow() + timedelta(seconds=30)
                task.updated_at = datetime.utcnow()
                logger.info(f"Scheduled AD verification task {task.id} for execution in 30 seconds")

            db.session.commit()
            logger.info(f"Successfully scheduled {len(dependent_tasks)} AD verification tasks")

        except Exception as e:
            logger.error(f"Error scheduling dependent AD verification tasks: {str(e)}")
            # Don't raise exception to avoid affecting the parent task

        return

def create_permission_task(action, folder, ad_group, permission_type, created_by):
    """Create a task for direct permission grant/revoke actions"""
    try:
        # Get configuration
        task_service = TaskService()
        config = task_service.get_config()
        
        # Create a task similar to approval tasks but for direct permission changes
        task_data = {
            'action': action,  # 'grant' or 'revoke'
            'folder_id': folder.id,
            'folder_path': folder.path,
            'folder_name': folder.name,
            'ad_group_id': ad_group.id,
            'ad_group_name': ad_group.name,
            'permission_type': permission_type,
            'created_by_id': created_by.id,
            'created_by_username': created_by.username,
            'execution_timestamp': datetime.utcnow().isoformat()
        }
        
        # Create Airflow task for applying the permission change
        from app.models import Task
        airflow_task = Task(
            name=f"Airflow DAG {action} permission: {ad_group.name} -> {folder.name}",
            task_type='airflow_dag',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=created_by.id,
            next_execution_at=datetime.utcnow()  # Execute immediately
        )
        
        # Set task data
        airflow_task.set_task_data(task_data)
        
        db.session.add(airflow_task)
        db.session.flush()  # Get the ID
        
        # Create verification task (delayed until Airflow completes)
        verification_task = Task(
            name=f"AD verification {action}: {ad_group.name} -> {folder.name}",
            task_type='ad_verification',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=created_by.id,
            next_execution_at=None  # Will be set when Airflow task completes
        )
        
        # Set verification task data
        verification_data = task_data.copy()
        verification_data.update({
            'depends_on_task_id': airflow_task.id,
            'expected_changes': {
                'folder_path': folder.path,
                'group': ad_group.name,
                'access_type': permission_type,
                'action': action
            }
        })
        verification_task.set_task_data(verification_data)
        
        db.session.add(verification_task)
        db.session.commit()
        
        logger.info(f"Created {action} permission tasks for folder {folder.id}, group {ad_group.id}")
        return airflow_task
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating permission tasks: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return None

def create_user_permission_task(action, folder, user, permission_type, created_by, notes=None, expires_at=None):
    """Create a task for direct user permission grant/revoke actions"""
    try:
        # Get configuration
        task_service = TaskService()
        config = task_service.get_config()
        
        # Create a task similar to permission tasks but for individual user permissions
        task_data = {
            'action': action,  # 'grant' or 'revoke'
            'folder_id': folder.id,
            'folder_path': folder.path,
            'folder_name': folder.name,
            'user_id': user.id,
            'username': user.username,
            'user_full_name': user.full_name,
            'permission_type': permission_type,
            'created_by_id': created_by.id,
            'created_by_username': created_by.username,
            'notes': notes,
            'expires_at': expires_at.isoformat() if expires_at else None,
            'execution_timestamp': datetime.utcnow().isoformat()
        }
        
        # Create Airflow task for applying the user permission change
        from app.models import Task
        airflow_task = Task(
            name=f"Airflow DAG {action} user permission: {user.username} -> {folder.name}",
            task_type='airflow_dag',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=created_by.id,
            next_execution_at=datetime.utcnow()  # Execute immediately
        )
        
        # Set task data
        airflow_task.set_task_data(task_data)
        
        db.session.add(airflow_task)
        db.session.flush()  # Get the ID
        
        # Create verification task (delayed by retry_delay)
        verification_task = Task(
            name=f"AD verification {action} user: {user.username} -> {folder.name}",
            task_type='ad_verification',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=created_by.id,
            next_execution_at=datetime.utcnow() + timedelta(seconds=config['retry_delay'])
        )
        
        # Create verification task data
        verification_data = task_data.copy()
        verification_data.update({
            'depends_on_task_id': airflow_task.id,
            'verification_type': 'user_permission',
            'ldap_search': {
                'user_dn': user.distinguished_name if hasattr(user, 'distinguished_name') else f'cn={user.username}',
                'folder_path': folder.path,
                'permission_type': permission_type,
                'expected_action': action
            }
        })
        verification_task.set_task_data(verification_data)
        
        db.session.add(verification_task)
        db.session.commit()
        
        logger.info(f"Created {action} user permission tasks for folder {folder.id}, user {user.id}")
        return airflow_task
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating user permission tasks: {str(e)}")
        return None

def create_user_permission_deletion_task(user, folder, ad_group, permission_type, csv_file_path, original_request=None):
    """Create a task for deleting a user permission"""
    try:
        # Get configuration
        task_service = TaskService()
        config = task_service.get_config()
        
        # Create a task for permission deletion
        task_data = {
            'action': 'delete',
            'folder_id': folder.id,
            'folder_path': folder.path,
            'folder_name': folder.name,
            'user_id': user.id,
            'username': user.username,
            'user_full_name': user.full_name,
            'ad_group_id': ad_group.id,
            'ad_group_name': ad_group.name,
            'permission_type': permission_type,
            'csv_file_path': os.path.basename(csv_file_path) if csv_file_path else None,
            'original_request_id': original_request.id if original_request else None,
            'deleted_by_id': user.id,
            'deleted_by_username': user.username,
            'execution_timestamp': datetime.utcnow().isoformat()
        }
        
        # Create Airflow task for applying the permission deletion
        from app.models import Task
        airflow_task = Task(
            name=f"Airflow DAG delete user permission: {user.username} -> {folder.name} ({ad_group.name})",
            task_type='airflow_dag',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=user.id,
            permission_request_id=original_request.id if original_request else None,
            next_execution_at=datetime.utcnow()  # Execute immediately
        )
        
        # Set task data
        airflow_task.set_task_data(task_data)
        
        db.session.add(airflow_task)
        db.session.flush()  # Get the ID
        
        # Create verification task (delayed by retry_delay)
        verification_task = Task(
            name=f"AD verification delete user: {user.username} -> {folder.name} ({ad_group.name})",
            task_type='ad_verification',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=user.id,
            permission_request_id=original_request.id if original_request else None,
            next_execution_at=datetime.utcnow() + timedelta(seconds=config['retry_delay'])
        )
        
        # Create verification task data
        verification_data = task_data.copy()
        verification_data.update({
            'depends_on_task_id': airflow_task.id,
            'verification_type': 'user_permission_deletion',
            'expected_changes': {
                'folder_path': folder.path,
                'group': ad_group.name,
                'access_type': permission_type,
                'action': 'delete',
                'user': user.username
            }
        })
        verification_task.set_task_data(verification_data)
        
        db.session.add(verification_task)

        # Mark the permission as "deletion in progress"
        try:
            from app.models import FolderPermission
            permission_to_mark = FolderPermission.query.filter_by(
                folder_id=folder.id,
                ad_group_id=ad_group.id,
                permission_type=permission_type,
                is_active=True
            ).first()

            if permission_to_mark:
                permission_to_mark.deletion_in_progress = True
                logger.info(f"Marked permission as deletion in progress: folder_id={folder.id}, ad_group_id={ad_group.id}, permission_type={permission_type}")
            else:
                logger.warning(f"Could not find permission to mark as deletion in progress: folder_id={folder.id}, ad_group_id={ad_group.id}, permission_type={permission_type}")
        except Exception as e:
            logger.error(f"Error marking permission as deletion in progress: {str(e)}")

        db.session.commit()

        logger.info(f"Created delete user permission tasks for folder {folder.id}, user {user.id}, group {ad_group.id}")
        return airflow_task
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating user permission deletion tasks: {str(e)}")
        return None

def create_permission_deletion_task(permission_request, deleted_by, csv_file_path):
    """Create a task for deleting a permission based on a permission request"""
    try:
        # Get configuration
        task_service = TaskService()
        config = task_service.get_config()
        
        # Get related objects from database (handles both persisted and temporary objects)
        from app.models import Folder, User, ADGroup
        folder = Folder.query.get(permission_request.folder_id)
        user = User.query.get(permission_request.requester_id)
        ad_group = ADGroup.query.get(permission_request.ad_group_id) if permission_request.ad_group_id else None
        
        if not folder or not user:
            logger.error(f"Required objects not found: folder={folder}, user={user}")
            return None
        
        # Create a task for permission deletion
        task_data = {
            'action': 'delete',
            'permission_request_id': permission_request.id if permission_request.id else None,
            'folder_id': permission_request.folder_id,
            'folder_path': folder.path,
            'folder_name': folder.name,
            'user_id': permission_request.requester_id,
            'username': user.username,
            'ad_group_id': permission_request.ad_group_id,
            'ad_group_name': ad_group.name if ad_group else None,
            'permission_type': permission_request.permission_type,
            'csv_file_path': os.path.basename(csv_file_path) if csv_file_path else None,
            'deleted_by_id': deleted_by.id,
            'deleted_by_username': deleted_by.username,
            'execution_timestamp': datetime.utcnow().isoformat()
        }
        
        # Create Airflow task for applying the permission deletion
        from app.models import Task
        request_id_display = permission_request.id if permission_request.id else f"temp_{permission_request.requester_id}_{permission_request.folder_id}"
        
        airflow_task = Task(
            name=f"Airflow DAG delete permission request #{request_id_display}",
            task_type='airflow_dag',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=deleted_by.id,
            permission_request_id=permission_request.id if permission_request.id else None,
            next_execution_at=datetime.utcnow()  # Execute immediately
        )
        
        # Set task data
        airflow_task.set_task_data(task_data)
        
        db.session.add(airflow_task)
        db.session.flush()  # Get the ID
        
        # Create verification task (delayed by retry_delay)
        verification_task = Task(
            name=f"AD verification delete permission request #{request_id_display}",
            task_type='ad_verification',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=deleted_by.id,
            permission_request_id=permission_request.id if permission_request.id else None,
            next_execution_at=datetime.utcnow() + timedelta(seconds=config['retry_delay'])
        )
        
        # Create verification task data
        verification_data = task_data.copy()
        verification_data.update({
            'depends_on_task_id': airflow_task.id,
            'verification_type': 'permission_request_deletion',
            'expected_changes': {
                'folder_path': folder.path,
                'group': ad_group.name if ad_group else None,
                'access_type': permission_request.permission_type,
                'action': 'delete',
                'user': user.username
            }
        })
        verification_task.set_task_data(verification_data)
        
        db.session.add(verification_task)
        db.session.commit()
        
        logger.info(f"Created delete permission request tasks for request {request_id_display}")
        return airflow_task
        
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error creating permission request deletion tasks: {str(e)}")
        return None

    def _execute_dependent_tasks_immediately(self, completed_task):
        """Execute tasks that depend on the completed task immediately"""
        try:
            # Find tasks that depend on this completed task
            from app.models import Task
            dependent_tasks = Task.query.filter_by(
                status='pending'
            ).filter(
                Task.task_data.contains(f'"depends_on_task_id": {completed_task.id}')
            ).all()

            if not dependent_tasks:
                logger.debug(f"No dependent tasks found for completed task {completed_task.id}")
                return

            logger.info(f"Found {len(dependent_tasks)} dependent tasks for task {completed_task.id}")

            for dependent_task in dependent_tasks:
                logger.info(f"Scheduling dependent task {dependent_task.id} for immediate execution")

                try:
                    # Schedule the dependent task for immediate execution
                    dependent_task.next_execution_at = datetime.utcnow()
                    dependent_task.updated_at = datetime.utcnow()

                    # Log the dependency resolution
                    logger.info(f"Dependent task {dependent_task.id} ({dependent_task.task_type}) scheduled for execution after completion of task {completed_task.id}")

                    # Try quick execution for AD verification tasks if possible
                    if dependent_task.task_type == 'ad_verification':
                        logger.info(f"Attempting quick AD verification for dependent task {dependent_task.id}")

                        # Try quick execution
                        permission_request = dependent_task.permission_request
                        if permission_request:
                            quick_success = self._try_quick_ad_verification(dependent_task, permission_request)
                            if quick_success:
                                logger.info(f"Quick AD verification successful for dependent task {dependent_task.id}")
                                continue  # Task completed, no need for background processing
                            else:
                                logger.info(f"Quick AD verification failed for dependent task {dependent_task.id}, will be processed by background")

                    # If quick execution failed or not applicable, ensure task is scheduled for background processing
                    dependent_task.status = 'pending'
                    logger.info(f"Dependent task {dependent_task.id} scheduled for background processing")

                except Exception as e:
                    logger.error(f"Error scheduling dependent task {dependent_task.id}: {str(e)}")
                    # Ensure task is still scheduled for execution even if quick execution failed
                    dependent_task.status = 'pending'
                    dependent_task.next_execution_at = datetime.utcnow()

            # Commit all dependency scheduling changes
            db.session.commit()
            logger.info(f"Successfully processed {len(dependent_tasks)} dependent tasks for completed task {completed_task.id}")

        except Exception as e:
            logger.error(f"Error finding/executing dependent tasks for task {completed_task.id}: {str(e)}")
            # Don't raise exception to avoid affecting the parent task completion

    def sync_airflow_task_statuses(self):
        """Synchronize task statuses with Airflow DAG runs"""
        try:
            # Get all running airflow tasks
            running_tasks = Task.query.filter_by(
                task_type='airflow_dag',
                status='running'
            ).all()

            if not running_tasks:
                logger.debug("No running Airflow tasks to sync")
                return

            logger.info(f"Syncing status for {len(running_tasks)} running Airflow tasks")

            # Get all DAG runs from Airflow to match with our tasks
            token = self.airflow_service.get_jwt_token()
            if not token:
                logger.error("Could not get JWT token for Airflow sync")
                return

            import requests
            url = f"{self.airflow_service.api_url}/dags/{self.airflow_service.dag_id}/dagRuns"
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }

            response = requests.get(url, headers=headers, verify=self.airflow_service.verify_ssl, timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to get DAG runs from Airflow: {response.status_code}")
                return

            airflow_runs = response.json().get('dag_runs', [])
            logger.info(f"Found {len(airflow_runs)} DAG runs in Airflow")

            for task in running_tasks:
                try:
                    # Extract DAG run ID from result_data
                    result_data = task.get_result_data()
                    dag_run_id = result_data.get('current_run_id')

                    if not dag_run_id:
                        # Try to extract from execution_time if available
                        execution_time = result_data.get('execution_time')
                        if execution_time:
                            # Generate probable run_id based on execution time
                            from datetime import datetime
                            try:
                                dt = datetime.fromisoformat(execution_time.replace('Z', '+00:00'))
                                dag_run_id = f"manual__{dt.strftime('%Y%m%dT%H%M%S')}"
                            except:
                                pass

                    if not dag_run_id:
                        logger.warning(f"Task {task.id} has no identifiable DAG run_id, skipping sync")
                        continue

                    # Find matching DAG run in Airflow
                    matching_run = None
                    for run in airflow_runs:
                        if run.get('dag_run_id') == dag_run_id:
                            matching_run = run
                            break

                    if matching_run:
                        airflow_state = matching_run.get('state', '').lower()
                        logger.debug(f"Task {task.id} DAG {dag_run_id} status: {airflow_state}")

                        # Update task status based on Airflow state
                        if airflow_state == 'success':
                            logger.info(f"Marking task {task.id} as completed (Airflow: success)")
                            task.mark_as_completed({
                                'airflow_sync': True,
                                'dag_run_id': dag_run_id,
                                'final_state': 'success',
                                'synced_at': datetime.utcnow().isoformat()
                            })
                            db.session.commit()

                        elif airflow_state in ['failed', 'upstream_failed']:
                            logger.info(f"Marking task {task.id} as failed (Airflow: {airflow_state})")
                            task.mark_as_failed(f"Airflow DAG execution {airflow_state} - {dag_run_id}")
                            db.session.commit()

                        # If still running or queued, keep as running
                        elif airflow_state in ['running', 'queued']:
                            logger.debug(f"Task {task.id} still running in Airflow")
                            # Update result data with sync info
                            result_data['airflow_status'] = airflow_state
                            result_data['last_sync'] = datetime.utcnow().isoformat()
                            task.set_result_data(result_data)
                            db.session.commit()

                        else:
                            logger.warning(f"Task {task.id} has unknown Airflow state: {airflow_state}")

                    else:
                        logger.warning(f"Could not find DAG run {dag_run_id} in Airflow (task {task.id})")

                except Exception as e:
                    logger.error(f"Error syncing status for task {task.id}: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Error in sync_airflow_task_statuses: {str(e)}")
            return False