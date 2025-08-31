from datetime import datetime
from app import db

class PermissionRequest(db.Model):
    __tablename__ = 'permission_requests'
    
    id = db.Column(db.Integer, primary_key=True)
    requester_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    folder_id = db.Column(db.Integer, db.ForeignKey('folders.id'), nullable=False)
    ad_group_id = db.Column(db.Integer, db.ForeignKey('ad_groups.id'), nullable=True)
    permission_type = db.Column(db.String(20), nullable=False)  # 'read', 'write'
    justification = db.Column(db.Text)
    business_need = db.Column(db.Text)
    status = db.Column(db.String(20), default='pending', nullable=False)  # 'pending', 'approved', 'rejected', 'canceled'
    validator_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    validation_comment = db.Column(db.Text)
    validation_date = db.Column(db.DateTime)
    expires_at = db.Column(db.DateTime)  # Optional expiration date for temporary access
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    requester = db.relationship('User', foreign_keys=[requester_id], backref='permission_requests')
    validator = db.relationship('User', foreign_keys=[validator_id], backref='validated_requests')
    folder = db.relationship('Folder', backref='permission_requests')
    ad_group = db.relationship('ADGroup', backref='permission_requests')
    
    # Constraints
    __table_args__ = (
        db.CheckConstraint(permission_type.in_(['read', 'write']), name='check_request_permission_type'),
        db.CheckConstraint(status.in_(['pending', 'approved', 'rejected', 'canceled', 'revoked']), name='check_request_status')
    )
    
    def __repr__(self):
        return f'<PermissionRequest {self.requester.username} - {self.folder.path} - {self.permission_type} - {self.status}>'
    
    def get_applicable_groups(self):
        """Get groups that would be assigned based on folder configuration and permission type"""
        from app.models.folder_permission import FolderPermission
        
        permissions = FolderPermission.query.filter_by(
            folder_id=self.folder_id,
            permission_type=self.permission_type,
            is_active=True
        ).all()
        
        return [perm.ad_group for perm in permissions]
    
    def assign_groups_automatically(self):
        """Assign AD groups automatically based on folder configuration"""
        applicable_groups = self.get_applicable_groups()
        
        # For now, assign to the first applicable group
        # In the future, this could be enhanced with user selection or other logic
        if applicable_groups:
            self.ad_group_id = applicable_groups[0].id
        
        return applicable_groups
    
    def approve(self, validator, comment=None):
        """Approve the permission request and create automation tasks"""
        # Ensure groups are assigned before approval
        if not self.ad_group_id:
            self.assign_groups_automatically()
        
        self.status = 'approved'
        self.validator = validator
        self.validation_comment = comment
        self.validation_date = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        # Generate CSV file for permission addition
        csv_file_path = None
        try:
            csv_file_path = self.generate_csv_file('add')
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to generate CSV for request {self.id}: {str(e)}")
        
        # Create automation tasks after approval
        try:
            from app.services.task_service import TaskService
            task_service = TaskService()
            tasks = task_service.create_approval_tasks(self, validator, csv_file_path)
            
            if tasks:
                # Log task creation
                from app.models.audit_event import AuditEvent
                AuditEvent.log_event(
                    user=validator,
                    event_type='task_creation',
                    action='approval_tasks_created',
                    resource_type='permission_request',
                    resource_id=self.id,
                    description=f'Tareas de automatización creadas para solicitud #{self.id}',
                    metadata={
                        'task_count': len(tasks),
                        'task_ids': [task.id for task in tasks],
                        'airflow_task': next((task.id for task in tasks if task.task_type == 'airflow_dag'), None),
                        'verification_task': next((task.id for task in tasks if task.task_type == 'ad_verification'), None),
                        'csv_file_path': csv_file_path
                    }
                )
                
        except Exception as e:
            # Log error but don't fail the approval
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to create automation tasks for request {self.id}: {str(e)}")
    
    def reject(self, validator, comment=None):
        """Reject the permission request"""
        self.status = 'rejected'
        self.validator = validator
        self.validation_comment = comment
        self.validation_date = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def is_pending(self):
        return self.status == 'pending'
    
    def is_approved(self):
        return self.status == 'approved'
    
    def is_rejected(self):
        return self.status == 'rejected'
    
    def is_canceled(self):
        return self.status == 'canceled'
    
    def cancel(self, user, comment=None):
        """Cancel the permission request"""
        self.status = 'canceled'
        self.validator = user
        self.validation_comment = comment or 'Solicitud cancelada por el usuario'
        self.validation_date = datetime.utcnow()
        self.updated_at = datetime.utcnow()
    
    def can_be_validated_by(self, user):
        """Check if user can validate this request"""
        # If there's a specific validator assigned, only that validator (or admins) can validate
        if self.validator_id:
            return user.is_admin() or user.id == self.validator_id
        
        # If no specific validator is assigned, any authorized validator for the folder can validate
        return user.can_validate_folder(self.folder)
    
    @staticmethod
    def check_existing_permissions(user_id, folder_id, requested_permission_type):
        """
        Check for existing permissions (manual approvals and AD sync) and determine action needed.
        
        Returns a dict with:
        - 'action': 'duplicate', 'change', or 'new'
        - 'existing_permission_type': if found
        - 'existing_source': 'manual' or 'ad_sync'
        - 'existing_request': the PermissionRequest object if from manual approval
        - 'message': description of what was found
        """
        from app.models.user import User
        from app.models.folder import Folder
        from app.models.user_ad_group import UserADGroupMembership
        from app.models.folder_permission import FolderPermission
        
        user = User.query.get(user_id)
        folder = Folder.query.get(folder_id)
        
        if not user or not folder:
            return {'action': 'error', 'message': 'Usuario o carpeta no encontrados'}
        
        # 1. Check for existing manual approvals (approved permission requests)
        existing_manual = PermissionRequest.query.filter_by(
            requester_id=user_id,
            folder_id=folder_id,
            status='approved'
        ).first()
        
        if existing_manual:
            if existing_manual.permission_type == requested_permission_type:
                return {
                    'action': 'duplicate',
                    'existing_permission_type': existing_manual.permission_type,
                    'existing_source': 'manual',
                    'existing_request': existing_manual,
                    'message': f'El usuario ya tiene permiso de {existing_manual.permission_type} aprobado manualmente para esta carpeta'
                }
            else:
                return {
                    'action': 'change',
                    'existing_permission_type': existing_manual.permission_type,
                    'existing_source': 'manual',
                    'existing_request': existing_manual,
                    'message': f'El usuario tiene permiso de {existing_manual.permission_type}. Se creará una solicitud para cambiar a {requested_permission_type}'
                }
        
        # 2. Check for AD-synchronized permissions
        # Get user's AD groups
        user_memberships = UserADGroupMembership.query.filter_by(
            user_id=user_id,
            is_active=True
        ).all()
        
        if user_memberships:
            user_ad_group_ids = [m.ad_group_id for m in user_memberships]
            
            # Check if any of these groups have permissions for this folder
            folder_permissions = FolderPermission.query.filter(
                FolderPermission.folder_id == folder_id,
                FolderPermission.ad_group_id.in_(user_ad_group_ids),
                FolderPermission.is_active == True
            ).all()
            
            for permission in folder_permissions:
                if permission.permission_type == requested_permission_type:
                    return {
                        'action': 'duplicate',
                        'existing_permission_type': permission.permission_type,
                        'existing_source': 'ad_sync',
                        'existing_group': permission.ad_group,
                        'message': f'El usuario ya tiene permiso de {permission.permission_type} a través del grupo AD {permission.ad_group.name}'
                    }
                else:
                    # Found a different permission type via AD
                    return {
                        'action': 'change',
                        'existing_permission_type': permission.permission_type,
                        'existing_source': 'ad_sync',
                        'existing_group': permission.ad_group,
                        'message': f'El usuario tiene permiso de {permission.permission_type} vía AD ({permission.ad_group.name}). Se creará solicitud para {requested_permission_type}'
                    }
        
        # 3. Check for pending requests
        pending_request = PermissionRequest.query.filter_by(
            requester_id=user_id,
            folder_id=folder_id,
            status='pending'
        ).first()
        
        if pending_request:
            if pending_request.permission_type == requested_permission_type:
                return {
                    'action': 'duplicate',
                    'existing_permission_type': pending_request.permission_type,
                    'existing_source': 'pending',
                    'existing_request': pending_request,
                    'message': f'Ya existe una solicitud pendiente de permiso {pending_request.permission_type} para esta carpeta'
                }
            else:
                return {
                    'action': 'change',
                    'existing_permission_type': pending_request.permission_type,
                    'existing_source': 'pending',
                    'existing_request': pending_request,
                    'message': f'Existe una solicitud pendiente de {pending_request.permission_type}. Se cancelará y se creará nueva solicitud de {requested_permission_type}'
                }
        
        # No existing permissions found
        return {
            'action': 'new',
            'message': f'Nueva solicitud de permiso {requested_permission_type}'
        }
    
    @staticmethod
    def create_permission_change_request(requester, folder_id, validator_id, new_permission_type, business_need, existing_permission_info):
        """
        Create a special permission request that handles changing from one permission type to another.
        This creates a request that, when approved, will remove the old permission and add the new one.
        """
        # Create the main permission request
        change_request = PermissionRequest(
            requester=requester,
            folder_id=folder_id,
            validator_id=validator_id,
            permission_type=new_permission_type,
            justification=business_need,
            business_need=business_need,
            expires_at=None
        )
        
        # Add metadata to track this is a change request
        change_request._is_change_request = True
        change_request._existing_permission_info = existing_permission_info
        
        return change_request
    
    def approve_with_change(self, validator, comment=None):
        """
        Approve a permission change request (remove old permission and add new one).
        This creates tasks for both removal and addition.
        """
        if not hasattr(self, '_is_change_request') or not self._is_change_request:
            # Regular approval for non-change requests
            return self.approve(validator, comment)
        
        # Ensure groups are assigned before approval
        if not self.ad_group_id:
            self.assign_groups_automatically()
        
        self.status = 'approved'
        self.validator = validator
        self.validation_comment = comment
        self.validation_date = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        # Create tasks for permission change (removal + addition)
        try:
            from app.services.task_service import TaskService
            task_service = TaskService()
            
            # Get existing permission info
            existing_info = getattr(self, '_existing_permission_info', {})
            
            # Create tasks for the permission change
            tasks = task_service.create_permission_change_tasks(self, validator, existing_info)
            
            if tasks:
                # Log task creation
                from app.models.audit_event import AuditEvent
                AuditEvent.log_event(
                    user=validator,
                    event_type='task_creation',
                    action='permission_change_tasks_created',
                    resource_type='permission_request',
                    resource_id=self.id,
                    description=f'Tareas de cambio de permiso creadas para solicitud #{self.id}',
                    metadata={
                        'task_count': len(tasks),
                        'task_ids': [task.id for task in tasks],
                        'old_permission_type': existing_info.get('existing_permission_type'),
                        'new_permission_type': self.permission_type,
                        'existing_source': existing_info.get('existing_source'),
                        'removal_task': next((task.id for task in tasks if 'remove' in task.description.lower()), None),
                        'addition_task': next((task.id for task in tasks if 'add' in task.description.lower()), None)
                    }
                )
                
        except Exception as e:
            # Log error but don't fail the approval
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to create permission change tasks for request {self.id}: {str(e)}")
        
        return tasks
    
    def to_dict(self):
        return {
            'id': self.id,
            'requester': self.requester.username,
            'requester_name': self.requester.full_name,
            'folder_path': self.folder.path,
            'folder_name': self.folder.name,
            'folder_description': self.folder.description,
            'ad_group_name': self.ad_group.name if self.ad_group else 'Asignación automática',
            'applicable_groups': [g.name for g in self.get_applicable_groups()],
            'permission_type': self.permission_type,
            'justification': self.justification,
            'business_need': self.business_need,
            'status': self.status,
            'validator': self.validator.username if self.validator else None,
            'validation_comment': self.validation_comment,
            'validation_date': self.validation_date.isoformat() if self.validation_date else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    def generate_csv_file(self, action='add'):
        """
        Generate CSV file for this permission request.
        
        Args:
            action: 'add' or 'remove'
        
        Returns:
            str: Path to generated CSV file
        """
        from app.services.csv_generator_service import CSVGeneratorService
        
        csv_service = CSVGeneratorService()
        return csv_service.generate_permission_change_csv(self, action)
    
    def generate_removal_csv(self):
        """Generate CSV file for removing this permission"""
        return self.generate_csv_file('remove')
    
    def revoke_permission(self, validator, comment=None):
        """
        Revoke an approved permission and generate removal CSV.
        This is used when we need to remove a previously granted permission.
        """
        if self.status != 'approved':
            raise ValueError("Only approved permissions can be revoked")
        
        # Generate CSV file for permission removal
        csv_file_path = None
        try:
            csv_file_path = self.generate_csv_file('remove')
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to generate removal CSV for request {self.id}: {str(e)}")
        
        # Update status
        self.status = 'revoked'
        self.validator = validator
        self.validation_comment = comment or 'Permiso revocado'
        self.validation_date = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        
        # Create automation tasks for removal
        try:
            from app.services.task_service import TaskService
            task_service = TaskService()
            tasks = task_service.create_revocation_tasks(self, validator, csv_file_path)
            
            if tasks:
                # Log task creation
                from app.models.audit_event import AuditEvent
                AuditEvent.log_event(
                    user=validator,
                    event_type='task_creation',
                    action='revocation_tasks_created',
                    resource_type='permission_request',
                    resource_id=self.id,
                    description=f'Tareas de revocación creadas para solicitud #{self.id}',
                    metadata={
                        'task_count': len(tasks),
                        'task_ids': [task.id for task in tasks],
                        'csv_file_path': csv_file_path
                    }
                )
                
        except Exception as e:
            # Log error but don't fail the revocation
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to create revocation tasks for request {self.id}: {str(e)}")
        
        return csv_file_path