import requests
import json
import csv
import os
from datetime import datetime, timedelta
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
        self.jwt_token = None  # Cache for JWT token
        self.token_expires_at = None  # Track token expiration
        self.airflow_version = None  # Cache for detected Airflow version
        self.auth_method = None  # Cache for detected auth method
        self.force_version = current_app.config.get('AIRFLOW_FORCE_VERSION', '')

    def _is_token_expired(self):
        """Check if the current JWT token is expired or about to expire"""
        if not self.token_expires_at:
            return True

        # Consider token expired if it expires in the next 5 minutes (buffer time)
        buffer_time = timedelta(minutes=5)
        return datetime.utcnow() >= (self.token_expires_at - buffer_time)

    def _detect_airflow_version(self):
        """Detect Airflow version and determine authentication method"""
        try:
            if self.airflow_version and self.auth_method:
                return self.auth_method  # Use cached result

            # Check if version is forced via configuration
            if self.force_version:
                if self.force_version == '2':
                    self.airflow_version = '2.x'
                    self.auth_method = 'basic'
                    logger.info("Forced Airflow 2.x version - using Basic authentication")
                    return self.auth_method
                elif self.force_version == '3':
                    self.airflow_version = '3.x'
                    self.auth_method = 'jwt'
                    logger.info("Forced Airflow 3.x version - using JWT authentication")
                    return self.auth_method

            if not self.api_url:
                logger.warning("Airflow API URL not configured for version detection")
                return None

            # Try to get version info from Airflow
            base_url = self.api_url.replace('/api/v2', '')
            version_url = f"{base_url}/api/v2/version"

            response = requests.get(
                version_url,
                timeout=self.timeout,
                verify=self.verify_ssl
            )

            if response.status_code == 200:
                version_data = response.json()
                version_string = version_data.get('version', '')
                logger.info(f"Auto-detected Airflow version: {version_string}")

                # Cache the version
                self.airflow_version = version_string

                # Determine auth method based on version
                if version_string.startswith('3.'):
                    self.auth_method = 'jwt'
                    logger.info("Using JWT authentication for Airflow 3.x")
                else:
                    self.auth_method = 'basic'
                    logger.info("Using Basic authentication for Airflow 2.x")

                return self.auth_method

            else:
                # Fallback: check API URL to determine version
                if '/api/v1' in self.api_url:
                    logger.warning(f"Could not detect Airflow version (status: {response.status_code}), but API URL contains v1 - using Basic Auth")
                    self.auth_method = 'basic'
                    return 'basic'
                else:
                    logger.warning(f"Could not detect Airflow version (status: {response.status_code}), defaulting to JWT")
                    self.auth_method = 'jwt'
                    return 'jwt'

        except Exception as e:
            # Fallback: check API URL to determine version
            if '/api/v1' in self.api_url:
                logger.warning(f"Error detecting Airflow version: {str(e)}, but API URL contains v1 - using Basic Auth")
                self.auth_method = 'basic'
                return 'basic'
            else:
                logger.warning(f"Error detecting Airflow version: {str(e)}, defaulting to JWT")
                self.auth_method = 'jwt'
                return 'jwt'

    def get_jwt_token(self, force_refresh=False):
        """Get JWT token for Airflow 3.0 authentication with intelligent caching"""
        try:
            # Check if we have a valid cached token
            if not force_refresh and self.jwt_token and not self._is_token_expired():
                logger.debug("Using cached JWT token")
                return self.jwt_token

            if not self.api_url or not self.username or not self.password:
                logger.warning("Airflow API URL, username or password not configured")
                return None

            logger.info("Generating new JWT token for Airflow authentication")

            # Remove /api/v2 or /api/v1 from base URL for auth endpoint
            base_url = self.api_url.replace('/api/v2', '').replace('/api/v1', '')
            auth_url = f"{base_url}/auth/token"

            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            payload = {
                'username': self.username,
                'password': self.password
            }

            response = requests.post(
                auth_url,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.timeout,
                verify=self.verify_ssl
            )

            if response.status_code in [200, 201]:
                token_data = response.json()
                self.jwt_token = token_data.get('access_token')

                # Calculate token expiration time
                # JWT tokens typically expire in 24 hours, but we'll be conservative and assume 1 hour
                # In a production environment, you could decode the JWT to get the actual expiration
                self.token_expires_at = datetime.utcnow() + timedelta(hours=1)

                logger.info(f"JWT token obtained successfully. Expires at: {self.token_expires_at}")
                return self.jwt_token
            else:
                logger.error(f"Failed to get JWT token: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"Error getting JWT token: {str(e)}")
            return None

    def _get_auth_headers(self, force_refresh=False):
        """Get authentication headers compatible with both Airflow 2.x and 3.x"""
        try:
            # Detect Airflow version and auth method
            auth_method = self._detect_airflow_version()

            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            if auth_method == 'jwt':
                # Airflow 3.x - Use JWT authentication
                jwt_token = self.get_jwt_token(force_refresh)
                if jwt_token:
                    headers['Authorization'] = f'Bearer {jwt_token}'
                    return headers
                else:
                    logger.error("Failed to obtain JWT token for Airflow 3.x")
                    return None

            elif auth_method == 'basic':
                # Airflow 2.x - Use Basic authentication
                if self.auth_token:
                    # Use pre-configured token
                    headers['Authorization'] = f'Basic {self.auth_token}'
                    return headers
                elif self.username and self.password:
                    # Use username/password for basic auth
                    import base64
                    credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
                    headers['Authorization'] = f'Basic {credentials}'
                    return headers
                else:
                    logger.error("No authentication credentials configured for Airflow 2.x")
                    return None

            else:
                logger.error("Could not determine Airflow authentication method")
                return None

        except Exception as e:
            logger.error(f"Error getting authentication headers: {str(e)}")
            return None

    def _create_dag_run_payload(self, run_id, conf):
        """Create DAG run payload compatible with both Airflow 2.x and 3.x"""
        payload = {
            'dag_run_id': run_id,
            'conf': conf or {}
        }

        # Airflow 3.x requires logical_date field
        if self.auth_method == 'jwt' or (self.airflow_version and self.airflow_version.startswith('3.')):
            current_time = datetime.utcnow()
            payload['logical_date'] = current_time.isoformat() + 'Z'

        return payload

    def invalidate_token_cache(self):
        """Manually invalidate the JWT token cache"""
        logger.info("Invalidating JWT token cache")
        self.jwt_token = None
        self.token_expires_at = None

    def trigger_dag(self, conf=None):
        """Trigger Airflow DAG execution compatible with both Airflow 2.x and 3.x"""
        try:
            if not self.api_url:
                logger.warning("Airflow API URL not configured")
                return False

            # Get authentication headers (auto-detects version)
            headers = self._get_auth_headers()
            if not headers:
                logger.error("Failed to obtain authentication headers for Airflow")
                return False

            url = f"{self.api_url}/dags/{self.dag_id}/dagRuns"

            # Generate unique run ID
            run_id = f"manual__{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"

            # Create payload compatible with detected version
            payload = self._create_dag_run_payload(run_id, conf)

            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.timeout,
                verify=self.verify_ssl
            )

            if response.status_code in [200, 201]:
                logger.info(f"Airflow DAG triggered successfully: {run_id}")
                return True
            elif response.status_code == 401:
                # Authentication failed, try once more with fresh credentials
                logger.warning("Authentication failed, refreshing credentials and retrying...")
                self.invalidate_token_cache()

                # Get fresh authentication headers
                fresh_headers = self._get_auth_headers(force_refresh=True)
                if not fresh_headers:
                    logger.error("Failed to obtain fresh authentication headers for retry")
                    return False

                # Retry the request
                retry_response = requests.post(
                    url,
                    headers=fresh_headers,
                    data=json.dumps(payload),
                    timeout=self.timeout,
                    verify=self.verify_ssl
                )

                if retry_response.status_code in [200, 201]:
                    logger.info(f"Airflow DAG triggered successfully on retry: {run_id}")
                    return True
                else:
                    logger.error(f"Failed to trigger Airflow DAG on retry: {retry_response.status_code} - {retry_response.text}")
                    return False
            else:
                logger.error(f"Failed to trigger Airflow DAG: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error triggering Airflow DAG: {str(e)}")
            # Note: Admin notification is now handled by TaskService after retry logic
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
        """Get status of a specific DAG run compatible with both Airflow 2.x and 3.x"""
        try:
            if not self.api_url:
                return None

            # Get authentication headers (auto-detects version)
            headers = self._get_auth_headers()
            if not headers:
                logger.error("Failed to obtain authentication headers for Airflow")
                return None

            # Remove Content-Type for GET request
            headers.pop('Content-Type', None)

            url = f"{self.api_url}/dags/{self.dag_id}/dagRuns/{run_id}"

            response = requests.get(
                url,
                headers=headers,
                timeout=self.timeout,
                verify=self.verify_ssl
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                # Authentication failed, try once more with fresh credentials
                logger.warning("Authentication failed for DAG status, refreshing credentials and retrying...")
                self.invalidate_token_cache()

                # Get fresh authentication headers
                fresh_headers = self._get_auth_headers(force_refresh=True)
                if not fresh_headers:
                    logger.error("Failed to obtain fresh authentication headers for DAG status retry")
                    return None

                # Remove Content-Type for GET request
                fresh_headers.pop('Content-Type', None)

                # Retry the request
                retry_response = requests.get(
                    url,
                    headers=fresh_headers,
                    timeout=self.timeout,
                    verify=self.verify_ssl
                )

                if retry_response.status_code == 200:
                    logger.info("DAG run status retrieved successfully on retry")
                    return retry_response.json()
                else:
                    logger.error(f"Failed to get DAG run status on retry: {retry_response.status_code}")
                    return None
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