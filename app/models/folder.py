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
                    'has_known_users': group_has_known_users
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