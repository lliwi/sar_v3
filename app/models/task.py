from datetime import datetime, timedelta
from app import db
import json
import os

class Task(db.Model):
    __tablename__ = 'tasks'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    task_type = db.Column(db.String(50), nullable=False)  # 'airflow_dag', 'ad_verification'
    status = db.Column(db.String(20), default='pending', nullable=False)  # 'pending', 'running', 'completed', 'failed', 'retry'
    
    # Task execution details
    attempt_count = db.Column(db.Integer, default=0)
    max_attempts = db.Column(db.Integer, default=1)
    next_execution_at = db.Column(db.DateTime, default=datetime.utcnow)
    delay_seconds = db.Column(db.Integer, default=0)  # Delay before execution
    
    # Task data and results
    task_data = db.Column(db.Text)  # JSON data for task execution
    result_data = db.Column(db.Text)  # JSON result from task execution
    error_message = db.Column(db.Text)
    
    # Related entities
    permission_request_id = db.Column(db.Integer, db.ForeignKey('permission_requests.id'), nullable=True)
    created_by_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    started_at = db.Column(db.DateTime)
    completed_at = db.Column(db.DateTime)
    
    # Relationships
    permission_request = db.relationship('PermissionRequest', backref='tasks')
    created_by = db.relationship('User', backref='created_tasks')
    
    # Constraints
    __table_args__ = (
        db.CheckConstraint(task_type.in_(['airflow_dag', 'ad_verification']), name='check_task_type'),
        db.CheckConstraint(status.in_(['pending', 'running', 'completed', 'failed', 'retry', 'cancelled']), name='check_task_status')
    )
    
    def __repr__(self):
        return f'<Task {self.name} - {self.task_type} - {self.status}>'
    
    def set_task_data(self, data):
        """Set task data as JSON"""
        self.task_data = json.dumps(data) if data else None
    
    def get_task_data(self):
        """Get task data from JSON"""
        try:
            return json.loads(self.task_data) if self.task_data else {}
        except json.JSONDecodeError:
            return {}
    
    def set_result_data(self, data):
        """Set result data as JSON"""
        self.result_data = json.dumps(data) if data else None
    
    def get_result_data(self):
        """Get result data from JSON"""
        try:
            return json.loads(self.result_data) if self.result_data else {}
        except json.JSONDecodeError:
            return {}
    
    def mark_as_running(self):
        """Mark task as running"""
        self.status = 'running'
        self.started_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def mark_as_completed(self, result_data=None):
        """Mark task as completed"""
        self.status = 'completed'
        self.completed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        if result_data:
            self.set_result_data(result_data)
    
    def mark_as_failed(self, error_message=None, result_data=None):
        """Mark task as failed"""
        self.status = 'failed'
        self.error_message = error_message
        self.updated_at = datetime.utcnow()
        if result_data:
            self.set_result_data(result_data)

        # Check if associated permission request should be marked as failed
        self._check_and_update_permission_request_status()
    
    def schedule_retry(self, delay_seconds=30):
        """Schedule task for retry"""
        self.attempt_count += 1

        if self.attempt_count >= self.max_attempts:
            # Don't call mark_as_failed here - let the calling code handle it
            # This prevents double-calling mark_as_failed and duplicate notifications
            return False

        self.status = 'retry'
        self.next_execution_at = datetime.utcnow() + timedelta(seconds=delay_seconds)
        self.updated_at = datetime.utcnow()
        return True

    def _check_and_update_permission_request_status(self):
        """
        Check if permission request should be marked as failed when tasks fail.
        Only mark as failed if ALL associated tasks have failed.
        """
        if not self.permission_request_id:
            return

        # Get all tasks for this permission request
        from app.models.permission_request import PermissionRequest
        permission_request = PermissionRequest.query.get(self.permission_request_id)
        if not permission_request or permission_request.status != 'approved':
            return

        all_tasks = Task.query.filter_by(
            permission_request_id=self.permission_request_id
        ).all()

        # Check if all tasks have failed or been cancelled
        if all_tasks:
            failed_tasks = [task for task in all_tasks if task.status in ['failed', 'cancelled']]
            pending_or_running_tasks = [task for task in all_tasks if task.status in ['pending', 'running', 'retry']]

            # Only mark permission request as failed if all tasks have failed/cancelled and none are pending/running
            if len(failed_tasks) == len(all_tasks) and len(pending_or_running_tasks) == 0:
                # Find a user to attribute the failure to (preferably the creator of this task)
                failed_by_user = self.created_by

                permission_request.mark_as_failed(
                    user=failed_by_user,
                    comment=f"Todas las tareas de automatización han fallado. Último error: {self.error_message}"
                )

                # Log the automatic failure
                from app.models.audit_event import AuditEvent
                AuditEvent.log_event(
                    user=failed_by_user,
                    event_type='permission_request',
                    action='auto_failed',
                    resource_type='permission_request',
                    resource_id=permission_request.id,
                    description=f'Solicitud #{permission_request.id} marcada automáticamente como fallida - todas las tareas fallaron',
                    metadata={
                        'failed_task_count': len(failed_tasks),
                        'total_task_count': len(all_tasks),
                        'last_error': self.error_message,
                        'folder_path': permission_request.folder.path,
                        'permission_type': permission_request.permission_type,
                        'requester': permission_request.requester.username
                    }
                )

    def increment_attempt_count(self):
        """Increment attempt count for immediate execution tracking"""
        self.attempt_count += 1
        self.updated_at = datetime.utcnow()

    def can_execute(self):
        """Check if task can be executed now"""
        if self.status not in ['pending', 'retry']:
            return False
        
        return datetime.utcnow() >= self.next_execution_at
    
    def is_pending(self):
        return self.status == 'pending'
    
    def is_running(self):
        return self.status == 'running'
    
    def is_completed(self):
        return self.status == 'completed'
    
    def is_failed(self):
        return self.status == 'failed'
    
    def is_retry(self):
        return self.status == 'retry'
    
    def is_cancelled(self):
        return self.status == 'cancelled'
    
    def can_be_cancelled(self):
        """Check if task can be cancelled (only pending, retry tasks)"""
        return self.status in ['pending', 'retry']
    
    def cancel(self, cancelled_by=None, reason=None):
        """Cancel a pending or retry task"""
        if not self.can_be_cancelled():
            raise ValueError(f"Cannot cancel task with status '{self.status}'")
        
        self.status = 'cancelled'
        self.updated_at = datetime.utcnow()
        self.error_message = reason or 'Task cancelled by user'
        
        # Store cancellation info in result_data
        cancellation_data = {
            'cancelled_at': datetime.utcnow().isoformat(),
            'cancelled_by': cancelled_by.username if cancelled_by else 'system',
            'cancellation_reason': reason or 'Task cancelled by user'
        }
        self.set_result_data(cancellation_data)
    
    def to_dict(self):
        from app.utils.timezone import utc_to_local

        # Convert UTC datetimes to local timezone for JSON serialization
        def format_datetime(dt):
            if dt:
                local_dt = utc_to_local(dt)
                return local_dt.isoformat() if local_dt else None
            return None

        return {
            'id': self.id,
            'name': self.name,
            'task_type': self.task_type,
            'status': self.status,
            'attempt_count': self.attempt_count,
            'max_attempts': self.max_attempts,
            'next_execution_at': format_datetime(self.next_execution_at),
            'delay_seconds': self.delay_seconds,
            'task_data': self.get_task_data(),
            'result_data': self.get_result_data(),
            'error_message': self.error_message,
            'permission_request_id': self.permission_request_id,
            'created_by': self.created_by.username if self.created_by else None,
            'created_at': format_datetime(self.created_at),
            'updated_at': format_datetime(self.updated_at),
            'started_at': format_datetime(self.started_at),
            'completed_at': format_datetime(self.completed_at)
        }

    @classmethod
    def create_airflow_task(cls, permission_request, created_by, csv_file_path=None):
        """Create an Airflow DAG execution task"""
        task = cls(
            name=f"Airflow DAG execution for request #{permission_request.id}",
            task_type='airflow_dag',
            permission_request_id=permission_request.id,
            created_by_id=created_by.id,
            max_attempts=3
        )
        
        task_data = {
            'dag_id': 'SAR_V3',
            'permission_request_id': permission_request.id,
            'folder_path': permission_request.folder.path,
            'ad_group_name': permission_request.ad_group.name if permission_request.ad_group else None,
            'permission_type': permission_request.permission_type,
            'requester': permission_request.requester.username,
            'validator': created_by.username,
            'csv_file_path': os.path.basename(csv_file_path) if csv_file_path else None  # Store only filename
        }
        task.set_task_data(task_data)
        
        return task
    
    @classmethod
    def create_ad_verification_task(cls, permission_request, created_by, delay_seconds=30):
        """Create an AD verification task"""
        task = cls(
            name=f"AD verification for request #{permission_request.id}",
            task_type='ad_verification',
            permission_request_id=permission_request.id,
            created_by_id=created_by.id,
            max_attempts=3,
            delay_seconds=delay_seconds,
            next_execution_at=datetime.utcnow() + timedelta(seconds=delay_seconds)
        )
        
        task_data = {
            'permission_request_id': permission_request.id,
            'folder_path': permission_request.folder.path,
            'ad_group_name': permission_request.ad_group.name if permission_request.ad_group else None,
            'permission_type': permission_request.permission_type,
            'expected_changes': {
                'group': permission_request.ad_group.name if permission_request.ad_group else None,
                'folder_path': permission_request.folder.path,
                'access_type': permission_request.permission_type
            }
        }
        task.set_task_data(task_data)
        
        return task