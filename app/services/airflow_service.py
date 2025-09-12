import requests
import json
import csv
import os
from datetime import datetime
from flask import current_app
# Remove celery import - functions are now synchronous
from app.models import PermissionRequest, FolderPermission
import logging

logger = logging.getLogger(__name__)

class AirflowService:
    def __init__(self):
        self.api_url = current_app.config.get('AIRFLOW_API_URL')
        self.username = current_app.config.get('AIRFLOW_USERNAME')
        self.password = current_app.config.get('AIRFLOW_PASSWORD')
        self.auth_token = current_app.config.get('AIRFLOW_AUTH_TOKEN')
        self.verify_ssl = current_app.config.get('AIRFLOW_VERIFY_SSL', False)
        self.timeout = int(current_app.config.get('AIRFLOW_TIMEOUT', 300))
        self.dag_id = current_app.config.get('AIRFLOW_DAG_NAME', 'SAR_V3')
        self.dag_name = self.dag_id  # Alias para consistencia
    
    def trigger_dag(self, conf=None):
        """Trigger Airflow DAG execution"""
        try:
            if not self.api_url:
                logger.warning("Airflow API URL not configured")
                return False
            
            url = f"{self.api_url}/dags/{self.dag_id}/dagRuns"
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            # Use Authorization header if token is available, otherwise use basic auth
            if self.auth_token:
                headers['Authorization'] = f'Basic {self.auth_token}'
            
            # Generate unique run ID
            run_id = f"manual__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
            
            payload = {
                'dag_run_id': run_id,
                'conf': conf or {}
            }
            
            # Configure request parameters
            request_params = {
                'url': url,
                'headers': headers,
                'data': json.dumps(payload),
                'timeout': self.timeout,
                'verify': self.verify_ssl
            }
            
            # Add auth if no token is provided
            if not self.auth_token and self.username and self.password:
                request_params['auth'] = (self.username, self.password)
            
            response = requests.post(**request_params)
            
            if response.status_code in [200, 201]:
                logger.info(f"Airflow DAG triggered successfully: {run_id}")
                return True
            else:
                logger.error(f"Failed to trigger Airflow DAG: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error triggering Airflow DAG: {str(e)}")
            # Send admin notification for Airflow errors
            try:
                from app.services.email_service import send_admin_error_notification
                send_admin_error_notification(
                    error_type="DAG_TRIGGER_FAILED",
                    service_name="Airflow",
                    error_message=f"Failed to trigger DAG {self.dag_id}: {str(e)}\nAPI URL: {self.api_url}"
                )
            except:
                pass  # Don't let notification errors break the main flow
            return False
    
    def create_permission_change_file(self, permission_requests):
        """Create CSV file with permission changes for Airflow"""
        try:
            # Create exports directory if it doesn't exist
            exports_dir = '/app/exports'
            os.makedirs(exports_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"permission_changes_{timestamp}.csv"
            filepath = os.path.join(exports_dir, filename)
            
            # Write CSV file
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'action',
                    'folder_path',
                    'ad_group_name',
                    'ad_group_dn',
                    'permission_type',
                    'requester',
                    'validator',
                    'request_id',
                    'validation_date'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for request in permission_requests:
                    writer.writerow({
                        'action': 'add_permission',
                        'folder_path': request.folder.path,
                        'ad_group_name': request.ad_group.name,
                        'ad_group_dn': request.ad_group.distinguished_name,
                        'permission_type': request.permission_type,
                        'requester': request.requester.username,
                        'validator': request.validator.username if request.validator else 'system',
                        'request_id': request.id,
                        'validation_date': request.validation_date.isoformat() if request.validation_date else ''
                    })
            
            logger.info(f"Permission change file created: {filepath}")
            return filename  # Return only filename instead of full path
            
        except Exception as e:
            logger.error(f"Error creating permission change file: {str(e)}")
            # Send admin notification for file creation errors
            try:
                from app.services.email_service import send_admin_error_notification
                send_admin_error_notification(
                    error_type="CSV_GENERATION_FAILED",
                    service_name="Airflow",
                    error_message=f"Failed to create permission change CSV file: {str(e)}"
                )
            except:
                pass
            return None
    
    def get_dag_run_status(self, run_id):
        """Get status of a specific DAG run"""
        try:
            if not self.api_url:
                return None
            
            url = f"{self.api_url}/dags/{self.dag_id}/dagRuns/{run_id}"
            
            headers = {
                'Accept': 'application/json'
            }
            
            # Use Authorization header if token is available
            if self.auth_token:
                headers['Authorization'] = f'Basic {self.auth_token}'
            
            # Configure request parameters
            request_params = {
                'url': url,
                'headers': headers,
                'timeout': self.timeout,
                'verify': self.verify_ssl
            }
            
            # Add auth if no token is provided
            if not self.auth_token and self.username and self.password:
                request_params['auth'] = (self.username, self.password)
            
            response = requests.get(**request_params)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get DAG run status: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting DAG run status: {str(e)}")
            return None

# @celery.task - removed for now, function is synchronous
def trigger_permission_changes(request_id):
    """Celery task to trigger Airflow DAG for permission changes"""
    try:
        from app import create_app
        app = create_app()
        
        with app.app_context():
            permission_request = PermissionRequest.query.get(request_id)
            if not permission_request:
                logger.error(f"Permission request {request_id} not found")
                return False
            
            if not permission_request.is_approved():
                logger.error(f"Permission request {request_id} is not approved")
                return False
            
            airflow_service = AirflowService()
            
            # Create change file with the single request
            change_file = airflow_service.create_permission_change_file([permission_request])
            if not change_file:
                logger.error(f"Failed to create change file for request {request_id}")
                return False
            
            # Trigger Airflow DAG
            # Now change_file contains only the filename
            conf = {
                'change_file': change_file,
                'request_ids': [request_id],
                'triggered_by': permission_request.validator.username if permission_request.validator else 'system'
            }
            
            success = airflow_service.trigger_dag(conf)
            
            if success:
                logger.info(f"Airflow DAG triggered for permission request {request_id}")
                
                # Send status notification to requester
                from app.services.email_service import send_permission_status_notification
                send_permission_status_notification(request_id, 'approved')
                
            return success
            
    except Exception as e:
        logger.error(f"Error triggering permission changes: {str(e)}")
        return False

# @celery.task - removed for now, function is synchronous
def batch_trigger_permission_changes(request_ids):
    """Celery task to trigger Airflow DAG for multiple permission changes"""
    try:
        from app import create_app
        app = create_app()
        
        with app.app_context():
            permission_requests = PermissionRequest.query.filter(
                PermissionRequest.id.in_(request_ids),
                PermissionRequest.status == 'approved'
            ).all()
            
            if not permission_requests:
                logger.error("No approved permission requests found")
                return False
            
            airflow_service = AirflowService()
            
            # Create change file with all requests
            change_file = airflow_service.create_permission_change_file(permission_requests)
            if not change_file:
                logger.error("Failed to create change file for batch requests")
                return False
            
            # Trigger Airflow DAG
            # Now change_file contains only the filename
            conf = {
                'change_file': change_file,
                'request_ids': request_ids,
                'triggered_by': 'batch_process'
            }
            
            success = airflow_service.trigger_dag(conf)
            
            if success:
                logger.info(f"Airflow DAG triggered for {len(request_ids)} permission requests")
                
                # Send status notifications to requesters
                from app.services.email_service import send_permission_status_notification
                for request in permission_requests:
                    send_permission_status_notification(request.id, 'approved')
            
            return success
            
    except Exception as e:
        logger.error(f"Error triggering batch permission changes: {str(e)}")
        return False

# @celery.task - removed for now, function is synchronous
def cleanup_old_export_files():
    """Celery task to cleanup old export files (older than 30 days)"""
    try:
        exports_dir = '/app/exports'
        if not os.path.exists(exports_dir):
            return True
        
        cutoff_time = datetime.utcnow().timestamp() - (30 * 24 * 60 * 60)  # 30 days
        cleaned_count = 0
        
        for filename in os.listdir(exports_dir):
            filepath = os.path.join(exports_dir, filename)
            if os.path.isfile(filepath):
                file_time = os.path.getmtime(filepath)
                if file_time < cutoff_time:
                    try:
                        os.remove(filepath)
                        cleaned_count += 1
                        logger.info(f"Removed old export file: {filename}")
                    except Exception as e:
                        logger.error(f"Error removing file {filename}: {str(e)}")
        
        logger.info(f"Cleanup completed. Removed {cleaned_count} old export files.")
        return True
        
    except Exception as e:
        logger.error(f"Error during export file cleanup: {str(e)}")
        return False