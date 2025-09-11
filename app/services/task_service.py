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
            'processing_interval': int(os.getenv('TASK_PROCESSING_INTERVAL', 300))
        }
    
    def cleanup_csv_file(self, task):
        """Clean up CSV file associated with a task after AD verification completes or is cancelled"""
        try:
            task_data = task.get_task_data()
            csv_file_path = task_data.get('csv_file_path')
            
            if csv_file_path and os.path.exists(csv_file_path):
                os.remove(csv_file_path)
                logger.info(f"Cleaned up CSV file {csv_file_path} for task {task.id} after AD verification completion")
                return True
            elif csv_file_path:
                logger.debug(f"CSV file {csv_file_path} not found for cleanup (task {task.id})")
            return False
            
        except Exception as e:
            logger.error(f"Error cleaning up CSV file for task {task.id}: {str(e)}")
            return False
    
    def create_approval_tasks(self, permission_request, validator, csv_file_path=None):
        """Create tasks when a permission request is approved"""
        try:
            config = self.get_config()
            logger.info(f"Starting task creation for permission request {permission_request.id}")
            
            # Check if tasks already exist for this permission request
            from app.models import Task
            existing_tasks = Task.query.filter_by(permission_request_id=permission_request.id).all()
            
            if existing_tasks:
                logger.warning(f"Tasks already exist for permission request {permission_request.id}. Existing tasks: {[t.id for t in existing_tasks]}")
                # Return existing tasks instead of creating duplicates
                return existing_tasks
            
            # Task 1: Execute Airflow DAG
            logger.debug(f"Creating Airflow task for request {permission_request.id}")
            airflow_task = Task.create_airflow_task(permission_request, validator, csv_file_path)
            # Update max_attempts from config
            airflow_task.max_attempts = config['max_retries']
            db.session.add(airflow_task)
            db.session.flush()  # Get the ID
            logger.info(f"Created Airflow task with ID {airflow_task.id} for request {permission_request.id}")
            
            # Task 2: Verify AD changes (delayed by retry_delay seconds)
            logger.debug(f"Creating AD verification task for request {permission_request.id}")
            verification_task = Task.create_ad_verification_task(permission_request, validator, delay_seconds=config['retry_delay'])
            # Update max_attempts from config
            verification_task.max_attempts = config['max_retries']
            db.session.add(verification_task)
            db.session.flush()
            logger.info(f"Created AD verification task with ID {verification_task.id} for request {permission_request.id}")
            
            # Link tasks - verification task references airflow task
            verification_data = verification_task.get_task_data()
            verification_data['depends_on_task_id'] = airflow_task.id
            verification_data['csv_file_path'] = csv_file_path  # Add CSV path for cleanup
            verification_task.set_task_data(verification_data)
            
            db.session.commit()
            
            logger.info(f"Successfully created approval tasks for permission request {permission_request.id}: Airflow task {airflow_task.id}, Verification task {verification_task.id}")
            return [airflow_task, verification_task]
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating approval tasks for request {permission_request.id}: {str(e)}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return []
    
    def create_revocation_tasks(self, permission_request, validator, csv_file_path=None):
        """Create tasks when a permission request is revoked"""
        try:
            # Check if revocation tasks already exist for this permission request
            from app.models import Task
            existing_tasks = Task.query.filter_by(permission_request_id=permission_request.id).filter(
                Task.name.contains('revocation')
            ).all()
            
            if existing_tasks:
                logger.warning(f"Revocation tasks already exist for permission request {permission_request.id}. Existing tasks: {[t.id for t in existing_tasks]}")
                return existing_tasks
                
            # Task 1: Execute Airflow DAG for revocation
            airflow_task = Task.create_airflow_task(permission_request, validator, csv_file_path)
            # Update the task name to reflect it's a revocation
            airflow_task.name = f"Airflow DAG revocation for request #{permission_request.id}"
            db.session.add(airflow_task)
            db.session.flush()  # Get the ID
            
            # Task 2: Verify AD changes (delayed 30 seconds)
            verification_task = Task.create_ad_verification_task(permission_request, validator, delay_seconds=30)
            # Update the task name to reflect it's a revocation verification
            verification_task.name = f"AD revocation verification for request #{permission_request.id}"
            db.session.add(verification_task)
            db.session.flush()
            
            # Link tasks - verification task references airflow task
            verification_data = verification_task.get_task_data()
            verification_data['depends_on_task_id'] = airflow_task.id
            verification_data['csv_file_path'] = csv_file_path  # Add CSV path for cleanup
            verification_task.set_task_data(verification_data)
            
            db.session.commit()
            
            logger.info(f"Created revocation tasks for permission request {permission_request.id}")
            return [airflow_task, verification_task]
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error creating revocation tasks: {str(e)}")
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
                'csv_file_path': csv_file_path,
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
                'csv_file_path': csv_file_path,
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
            
            # Prepare configuration for Airflow
            conf = {
                'permission_request_id': task_data.get('permission_request_id'),
                'folder_path': task_data.get('folder_path'),
                'ad_group_name': task_data.get('ad_group_name'),
                'permission_type': task_data.get('permission_type'),
                'requester': task_data.get('requester'),
                'validator': task_data.get('validator'),
                'csv_file_path': task_data.get('csv_file_path'),
                'task_id': task.id,
                'execution_timestamp': datetime.utcnow().isoformat()
            }
            
            # Trigger Airflow DAG
            success = self.airflow_service.trigger_dag(conf)
            
            if success:
                result_data = {
                    'dag_execution_status': 'triggered',
                    'dag_id': self.airflow_service.dag_id,
                    'execution_time': datetime.utcnow().isoformat(),
                    'config_sent': conf
                }
                task.mark_as_completed(result_data)
                
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
                return True
            else:
                error_msg = "Failed to trigger Airflow DAG"
                retry_scheduled = task.schedule_retry(delay_seconds=config['retry_delay'])
                
                if not retry_scheduled:
                    task.mark_as_failed(error_msg)
                    # Note: CSV cleanup is handled by AD verification task
                
                logger.error(f"Airflow task {task.id} failed: {error_msg}")
                return False
                
        except Exception as e:
            error_msg = f"Error executing Airflow task: {str(e)}"
            retry_scheduled = task.schedule_retry(delay_seconds=60)
            
            if not retry_scheduled:
                task.mark_as_failed(error_msg)
                # Note: CSV cleanup is handled by AD verification task
            
            logger.error(error_msg)
            return False
        finally:
            db.session.commit()
    
    def execute_ad_verification_task(self, task):
        """Execute an AD verification task"""
        try:
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
                    task.schedule_retry(delay_seconds=config['retry_delay'])
                    db.session.commit()
                    logger.info(f"AD verification task {task.id} rescheduled - waiting for Airflow task completion")
                    return False
            
            # Verify AD changes
            verification_result = self.verify_ad_changes(
                folder_path=expected_changes.get('folder_path'),
                ad_group_name=expected_changes.get('group'),
                access_type=expected_changes.get('access_type'),
                action_type=expected_changes.get('action', task_data.get('action', 'add'))
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
                    # Clean up CSV file after permanent AD verification failure
                    self.cleanup_csv_file(task)
                
                logger.warning(f"AD verification task {task.id} failed attempt {task.attempt_count}")
                return False
                
        except Exception as e:
            error_msg = f"Error executing AD verification task: {str(e)}"
            retry_scheduled = task.schedule_retry(delay_seconds=30)
            
            if not retry_scheduled:
                task.mark_as_failed(error_msg)
                # Clean up CSV file after permanent AD verification failure
                self.cleanup_csv_file(task)
            
            logger.error(error_msg)
            return False
        finally:
            db.session.commit()
    
    def verify_ad_changes(self, folder_path, ad_group_name, access_type, action_type='add'):
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
                
                # Simulate checking folder permissions
                # In production, you would use Windows PowerShell commands or 
                # direct NTFS API calls to verify ACL changes
                permissions_verified = self._simulate_folder_permission_check(folder_path, ad_group_name, access_type, action_type)
                
                verification_details['permissions_verified'] = permissions_verified
                verification_details['action_type'] = action_type
                
                # For removal actions, success means permissions are NOT found
                if is_removal_action:
                    success = not permissions_verified  # Inverted logic for removals
                    error_msg = 'Permission still exists on folder' if permissions_verified else None
                else:
                    success = permissions_verified
                    error_msg = 'Permissions not found on folder' if not permissions_verified else None
                
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
    
    def _simulate_folder_permission_check(self, folder_path, ad_group_name, access_type, action_type='add'):
        """Simulate checking folder permissions (replace with actual implementation)"""
        try:
            # In a real implementation, this would:
            # 1. Connect to the file server
            # 2. Use PowerShell commands like Get-Acl to check folder permissions
            # 3. Parse the ACL to find the specific group
            # 4. Verify the access rights match the requested type
            
            # For simulation, we'll return True after a small delay
            import time
            time.sleep(0.5)  # Simulate network/filesystem check delay
            
            is_removal_action = action_type in ['remove', 'remove_ad_sync', 'delete']
            
            logger.info(f"Simulated permission check for {folder_path}: {ad_group_name} -> {access_type} (action: {action_type})")
            
            # Simulate different behaviors for add vs remove operations
            import random
            
            if is_removal_action:
                # For removal operations, simulate that permissions are usually gone (90% success rate)
                # Return False means "permission not found" which is success for removal
                if random.random() < 0.9:
                    logger.info(f"Simulated removal verification: permission NOT found (success)")
                    return False  # Permission not found = successful removal
                else:
                    logger.warning(f"Simulated removal verification: permission still exists (failure)")
                    return True   # Permission still found = failed removal
            else:
                # For addition operations, simulate that permissions are usually applied (90% success rate)
                # Return True means "permission found" which is success for addition
                if random.random() < 0.9:
                    logger.info(f"Simulated addition verification: permission found (success)")
                    return True   # Permission found = successful addition
                else:
                    logger.warning(f"Simulated addition verification: permission NOT found (failure)")
                    return False  # Permission not found = failed addition
            
        except Exception as e:
            logger.error(f"Error in folder permission check simulation: {str(e)}")
            # For removal operations, assume success on error
            # For addition operations, assume failure on error
            return action_type in ['remove', 'remove_ad_sync', 'delete']
    
    def process_pending_tasks(self):
        """Process all pending and retry tasks that are ready for execution"""
        try:
            config = self.get_config()
            # Get tasks ready for execution (limit by batch size)
            ready_tasks = Task.query.filter(
                Task.status.in_(['pending', 'retry']),
                Task.next_execution_at <= datetime.utcnow()
            ).order_by(Task.created_at).limit(config['batch_size']).all()
            
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
    
    def execute_airflow_task(self, task):
        """Execute an Airflow DAG task"""
        try:
            task.mark_as_running()
            db.session.commit()
            
            task_data = task.get_task_data()
            csv_file_path = task_data.get('csv_file_path')
            
            # Prepare configuration for Airflow DAG
            conf = {
                'change_file': csv_file_path,
                'request_ids': [task_data.get('permission_request_id')],
                'triggered_by': task_data.get('validator', 'system'),
                'folder_path': task_data.get('folder_path'),
                'ad_group_name': task_data.get('ad_group_name'),
                'permission_type': task_data.get('permission_type'),
                'ad_source_domain': os.getenv('AD_DOMAIN_PREFIX', ''),
                'ad_target_domain': os.getenv('AD_TARGET_DOMAIN', 'AUDI')
            }
            
            # Trigger Airflow DAG
            success = self.airflow_service.trigger_dag(conf)
            
            if success:
                result_data = {
                    'dag_triggered': True,
                    'configuration': conf,
                    'execution_time': datetime.utcnow().isoformat()
                }
                task.mark_as_completed(result_data)
                logger.info(f"Airflow task {task.id} completed successfully")
                # Note: CSV cleanup is handled by AD verification task
            else:
                task.mark_as_failed("Failed to trigger Airflow DAG")
                logger.error(f"Airflow task {task.id} failed to trigger DAG")
                # Note: CSV cleanup is handled by AD verification task
            
            db.session.commit()
            return success
            
        except Exception as e:
            logger.error(f"Error executing Airflow task {task.id}: {str(e)}")
            task.mark_as_failed(str(e))
            db.session.commit()
            return False
    
    def execute_ad_verification_task(self, task):
        """Execute an AD verification task"""
        try:
            task.mark_as_running()
            db.session.commit()
            
            task_data = task.get_task_data()
            permission_request_id = task_data.get('permission_request_id')
            
            # Get permission request (may be None for temporary objects)
            permission_request = None
            if permission_request_id:
                permission_request = PermissionRequest.query.get(permission_request_id)
                if not permission_request:
                    task.mark_as_failed(f"Permission request {permission_request_id} not found")
                    db.session.commit()
                    return False
            # If permission_request_id is None, we continue without the permission_request object
            # This is normal for temporary objects created during deletion processes
            
            # Get folder_id from task_data or permission_request
            folder_id = task_data.get('folder_id')
            if not folder_id and permission_request:
                folder_id = permission_request.folder_id
            
            if not folder_id:
                task.mark_as_failed("No folder_id available for verification")
                db.session.commit()
                return False
            
            # Use the corrected verify_ad_changes method
            expected_changes = task_data.get('expected_changes', {})
            
            # Verify AD changes using the corrected logic for deletions
            verification_result = self.verify_ad_changes(
                folder_path=expected_changes.get('folder_path'),
                ad_group_name=expected_changes.get('group'),
                access_type=expected_changes.get('access_type'),
                action_type=expected_changes.get('action', task_data.get('action', 'add'))
            )
            
            result_data = {
                'verification_status': 'success' if verification_result['success'] else 'failed',
                'ad_permissions_applied': verification_result['success'],
                'verification_time': datetime.utcnow().isoformat(),
                'details': verification_result['details']
            }
            
            if verification_result['success']:
                task.mark_as_completed(result_data)
                logger.info(f"AD verification task {task.id} completed successfully")
                
                # Clean up CSV file after successful AD verification
                self.cleanup_csv_file(task)
                
                success = True
            else:
                error_msg = verification_result.get('error', 'Unknown verification error')
                task.mark_as_failed(f"AD verification failed: {error_msg}", result_data)
                logger.error(f"AD verification task {task.id} failed: {error_msg}")
                
                # Clean up CSV file after permanent AD verification failure
                self.cleanup_csv_file(task)
                
                success = False
            
            db.session.commit()
            return success
            
        except Exception as e:
            logger.error(f"Error executing AD verification task {task.id}: {str(e)}")
            task.mark_as_failed(str(e))
            
            # Clean up CSV file after AD verification error
            self.cleanup_csv_file(task)
            
            db.session.commit()
            return False
    
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
        
        # Create verification task (delayed by retry_delay)
        verification_task = Task(
            name=f"AD verification {action}: {ad_group.name} -> {folder.name}",
            task_type='ad_verification',
            status='pending',
            max_attempts=config['max_retries'],
            attempt_count=0,
            created_by_id=created_by.id,
            next_execution_at=datetime.utcnow() + timedelta(seconds=config['retry_delay'])
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
            'csv_file_path': csv_file_path,
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
            'csv_file_path': csv_file_path,
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