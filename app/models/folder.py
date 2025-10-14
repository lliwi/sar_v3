from datetime import datetime
from app import db

class Folder(db.Model):
    __tablename__ = 'folders'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    path = db.Column(db.String(500), unique=True, nullable=False, index=True)
    description = db.Column(db.Text)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_by_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    created_by = db.relationship('User', foreign_keys=[created_by_id], backref='created_folders')
    permissions = db.relationship('FolderPermission', backref='folder', lazy=True, cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Folder {self.path}>'

    @property
    def folder_name(self):
        """Extract folder name from path"""
        import os
        # Remove trailing slashes/backslashes and get basename
        basename = os.path.basename(self.path.rstrip('/\\'))
        # If basename is empty (root path), use the name field or last meaningful part
        if not basename:
            return self.name if self.name else self.path
        return basename

    @property
    def sanitized_path(self):
        """Get path with sanitized backslashes"""
        import re

        # Simple approach: normalize all multiple backslashes first
        # Then ensure UNC paths have exactly 2 backslashes at start

        # Step 1: Replace any sequence of 2+ backslashes with single backslash
        sanitized = re.sub(r'\\{2,}', r'\\', self.path)

        # Step 2: If this looks like it should be a UNC path (starts with \server)
        # then add the missing backslash at the beginning
        if sanitized.startswith('\\') and not sanitized.startswith('\\\\'):
            # Check if the part after the first \ looks like a server name
            parts = sanitized[1:].split('\\', 1)
            if len(parts) > 0 and parts[0] and not parts[0].startswith('/'):
                # This looks like a UNC path, add the missing backslash
                sanitized = '\\' + sanitized

        # Also handle forward slashes that might be duplicated
        sanitized = re.sub(r'/+', '/', sanitized)
        return sanitized
    
    def get_permissions_by_type(self, permission_type):
        """Get all groups with specific permission type for this folder"""
        return [fp for fp in self.permissions if fp.permission_type == permission_type]
    
    def has_permission(self, ad_group, permission_type):
        """Check if an AD group has specific permission on this folder"""
        return any(
            fp.ad_group_id == ad_group.id and fp.permission_type == permission_type
            for fp in self.permissions
        )
    
    def get_users_with_permissions(self):
        """Get all users who have permissions to this folder through AD group membership"""
        from .user_ad_group import UserADGroupMembership
        from .user import User
        
        # Get all active permissions for this folder
        active_permissions = [fp for fp in self.permissions if fp.is_active]
        
        # Get all users who belong to the groups that have permissions
        users_with_permissions = {}
        
        for permission in active_permissions:
            # Find users who belong to this AD group
            memberships = UserADGroupMembership.query.filter_by(
                ad_group_id=permission.ad_group_id,
                is_active=True
            ).all()
            
            for membership in memberships:
                if membership.user.is_active:
                    user_key = membership.user.id
                    if user_key not in users_with_permissions:
                        users_with_permissions[user_key] = {
                            'user': membership.user,
                            'permissions': [],
                            'groups': []
                        }
                    
                    # Add this permission
                    perm_info = {
                        'permission_id': permission.id,
                        'permission_type': permission.permission_type,
                        'ad_group': permission.ad_group,
                        'granted_at': permission.granted_at,
                        'granted_by': permission.granted_by
                    }
                    
                    users_with_permissions[user_key]['permissions'].append(perm_info)
                    
                    # Add group info if not already added
                    group_info = {
                        'id': permission.ad_group.id,
                        'name': permission.ad_group.name,
                        'permission_type': permission.permission_type
                    }
                    
                    if group_info not in users_with_permissions[user_key]['groups']:
                        users_with_permissions[user_key]['groups'].append(group_info)
        
        return list(users_with_permissions.values())
    
    def get_all_users_with_permissions(self):
        """Get all users who have permissions (both through AD groups and direct assignments)"""
        from .user_folder_permission import UserFolderPermission
        
        users_permissions = {}
        
        # 1. Get users with permissions through AD groups
        ad_group_users = self.get_users_with_permissions()
        for user_data in ad_group_users:
            user_id = user_data['user'].id
            users_permissions[user_id] = {
                'user': user_data['user'],
                'permissions': [],
                'source': []  # Track where permissions come from
            }
            
            # Add AD group permissions
            for perm in user_data['permissions']:
                perm_detail = {
                    'type': perm['permission_type'],
                    'source': 'ad_group',
                    'source_name': perm['ad_group'].name,
                    'granted_at': perm['granted_at'],
                    'granted_by': perm['granted_by'] if perm['granted_by'] else None,
                    'permission_id': perm['permission_id']
                }
                users_permissions[user_id]['permissions'].append(perm_detail)
                users_permissions[user_id]['source'].append(f"Grupo AD: {perm['ad_group'].name}")
        
        # 2. Get users with direct permissions
        direct_permissions = UserFolderPermission.query.filter_by(
            folder_id=self.id,
            is_active=True
        ).all()
        
        for direct_perm in direct_permissions:
            if direct_perm.user.is_active:
                user_id = direct_perm.user.id
                
                if user_id not in users_permissions:
                    users_permissions[user_id] = {
                        'user': direct_perm.user,
                        'permissions': [],
                        'source': []
                    }
                
                # Add direct permission
                perm_detail = {
                    'type': direct_perm.permission_type,
                    'source': 'direct',
                    'source_name': 'Asignación Directa',
                    'granted_at': direct_perm.granted_at,
                    'granted_by': direct_perm.granted_by.username if direct_perm.granted_by else None,
                    'permission_id': direct_perm.id,
                    'expires_at': direct_perm.expires_at,
                    'notes': direct_perm.notes
                }
                users_permissions[user_id]['permissions'].append(perm_detail)
                users_permissions[user_id]['source'].append("Asignación Directa")
        
        # Convert to list and sort by username
        result = list(users_permissions.values())
        result.sort(key=lambda x: x['user'].username.lower())
        
        return result
    
    def has_user_deletion_in_progress(self, user_id):
        """Check if a specific user has deletion or permission change in progress for this folder"""
        from app.models.task import Task
        from app.models.user import User
        from sqlalchemy import and_, or_
        import json

        # Get username for the user
        user = User.query.get(user_id)
        if not user:
            return False

        # Get all active tasks that might be related to this folder and user
        # We cast to a broader net to avoid missing tasks due to JSON formatting differences
        active_tasks = Task.query.filter(
            and_(
                Task.status.in_(['pending', 'processing', 'retry']),
                or_(
                    Task.task_type == 'airflow_dag',
                    Task.task_type == 'ad_verification'
                )
            )
        ).all()

        # Check each task to see if it matches this user and folder
        for task in active_tasks:
            try:
                task_data = json.loads(task.task_data)

                # Check if task is for this folder (by folder_id or folder_path)
                task_folder_id = task_data.get('folder_id')
                task_folder_path = task_data.get('folder_path', '').replace('\\\\', '\\')

                is_same_folder = (
                    task_folder_id == self.id or
                    task_folder_path == self.path
                )

                if not is_same_folder:
                    continue

                # Check if this task is for the specific user
                # Check multiple possible identifiers
                task_user_id = task_data.get('user_id')
                task_requester = task_data.get('requester', '')

                is_same_user = (
                    task_user_id == user_id or
                    task_requester == user.username or
                    task_requester == user.full_name
                )

                if not is_same_user:
                    continue

                # If we get here, the task is for this user and folder
                # Check the action to determine if it's relevant
                action = task_data.get('action', '')

                # These actions indicate an in-progress operation
                relevant_actions = [
                    'delete', 'add', 'remove_ad_sync', 'remove_direct',
                    'remove_existing', 'remove_manual'
                ]

                if action in relevant_actions or not action:
                    return True

            except Exception as e:
                # Log but don't fail - continue checking other tasks
                import logging
                logger = logging.getLogger(__name__)
                logger.debug(f"Error parsing task {task.id} data: {str(e)}")
                continue

        return False

    def get_user_task_type_in_progress(self, user_id):
        """Get the type of task in progress for a user (deletion or change)"""
        from app.models.task import Task
        from app.models.user import User
        from sqlalchemy import and_, or_
        import json

        # Get username for the user
        user = User.query.get(user_id)
        if not user:
            return None

        # Get all active tasks that might be related to this folder and user
        all_active_tasks = Task.query.filter(
            and_(
                Task.status.in_(['pending', 'processing', 'retry']),
                or_(
                    Task.task_type == 'airflow_dag',
                    Task.task_type == 'ad_verification'
                )
            )
        ).all()

        # Filter tasks for this folder and user
        folder_tasks = []
        for task in all_active_tasks:
            try:
                task_data = json.loads(task.task_data)

                # Check if task is for this folder
                task_folder_id = task_data.get('folder_id')
                task_folder_path = task_data.get('folder_path', '').replace('\\\\', '\\')

                is_same_folder = (
                    task_folder_id == self.id or
                    task_folder_path == self.path
                )

                if not is_same_folder:
                    continue

                # Check if this task is for the specific user
                task_user_id = task_data.get('user_id')
                task_requester = task_data.get('requester', '')

                is_same_user = (
                    task_user_id == user_id or
                    task_requester == user.username or
                    task_requester == user.full_name
                )

                if is_same_user:
                    folder_tasks.append(task)

            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.debug(f"Error parsing task {task.id} data: {str(e)}")
                continue

        if not folder_tasks:
            return None

        # Check if there are both delete and add tasks (indicates a change)
        has_delete = False
        has_add = False

        for task in folder_tasks:
            try:
                task_data = json.loads(task.task_data)
                action = task_data.get('action', '')

                # Removal actions
                if action in ('delete', 'remove_ad_sync', 'remove_direct', 'remove_existing', 'remove_manual'):
                    has_delete = True
                # Addition actions
                elif action == 'add' or not action:
                    # Tasks without action field are typically addition tasks
                    has_add = True
            except:
                pass

        # If we have both delete and add, it's a change
        if has_delete and has_add:
            return 'change'
        # If only delete, it's a deletion
        elif has_delete:
            return 'delete'
        # If only add, it's likely an addition
        elif has_add:
            return 'add'

        return None

    def get_permissions_summary(self):
        """Get a comprehensive summary of all permissions including AD groups and users"""
        summary = {
            'users_with_permissions': [],
            'ad_groups_with_permissions': [],
            'total_permissions': 0
        }
        
        # Get users with confirmed permissions (through AD group membership sync)
        users_with_permissions = self.get_all_users_with_permissions()
        summary['users_with_permissions'] = users_with_permissions
        
        # Get AD groups that have permissions (regardless of whether we know the members)
        from .user_folder_permission import UserFolderPermission
        
        # Get AD group permissions
        ad_group_permissions = []
        for permission in self.permissions:
            if permission.is_active:
                # Check if we already have users for this group
                group_has_known_users = any(
                    any(p['source'] == 'ad_group' and p['source_name'] == permission.ad_group.name
                        for p in user_data['permissions'])
                    for user_data in users_with_permissions
                )

                ad_group_info = {
                    'ad_group': permission.ad_group,
                    'permission_type': permission.permission_type,
                    'granted_at': permission.granted_at,
                    'granted_by': permission.granted_by,
                    'permission_id': permission.id,
                    'has_known_users': group_has_known_users,
                    'deletion_in_progress': permission.deletion_in_progress
                }
                ad_group_permissions.append(ad_group_info)
        
        summary['ad_groups_with_permissions'] = ad_group_permissions
        summary['total_permissions'] = len(users_with_permissions) + len([
            g for g in ad_group_permissions if not g['has_known_users']
        ])
        
        return summary
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'path': self.path,
            'description': self.description,
            'is_active': self.is_active,
            'owners': [owner.to_dict() for owner in self.owners],
            'validators': [validator.to_dict() for validator in self.validators],
            'permissions': [perm.to_dict() for perm in self.permissions]
        }