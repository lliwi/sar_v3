from flask_login import UserMixin
from datetime import datetime
from app import db

user_roles = db.Table('user_roles',
    db.Column('user_id', db.Integer, db.ForeignKey('users.id'), primary_key=True),
    db.Column('role_id', db.Integer, db.ForeignKey('roles.id'), primary_key=True)
)

folder_owners = db.Table('folder_owners',
    db.Column('user_id', db.Integer, db.ForeignKey('users.id'), primary_key=True),
    db.Column('folder_id', db.Integer, db.ForeignKey('folders.id'), primary_key=True)
)

folder_validators = db.Table('folder_validators',
    db.Column('user_id', db.Integer, db.ForeignKey('users.id'), primary_key=True),
    db.Column('folder_id', db.Integer, db.ForeignKey('folders.id'), primary_key=True)
)

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    full_name = db.Column(db.String(200), nullable=False)
    department = db.Column(db.String(100))
    distinguished_name = db.Column(db.String(500))  # LDAP DN
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    last_login = db.Column(db.DateTime)
    last_sync = db.Column(db.DateTime)  # Last LDAP synchronization
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    roles = db.relationship('Role', secondary=user_roles, lazy='subquery',
                           backref=db.backref('users', lazy=True))
    owned_folders = db.relationship('Folder', secondary=folder_owners, lazy='subquery',
                                   backref=db.backref('owners', lazy=True))
    validated_folders = db.relationship('Folder', secondary=folder_validators, lazy='subquery',
                                       backref=db.backref('validators', lazy=True))
    
    def __repr__(self):
        return f'<User {self.username}>'
    
    def has_role(self, role_name):
        return any(role.name == role_name for role in self.roles)
    
    def is_admin(self):
        return self.has_role('Administrador')
    
    def can_validate_folder(self, folder):
        return self.is_admin() or self in folder.owners or self in folder.validators
    
    def get_active_ad_groups(self):
        """Get active AD groups this user belongs to"""
        from .user_ad_group import UserADGroupMembership
        memberships = UserADGroupMembership.query.filter_by(
            user_id=self.id, 
            is_active=True
        ).all()
        return [membership.ad_group for membership in memberships if membership.ad_group.is_active]
    
    def has_permission_to_folder(self, folder, permission_type):
        """Check if user has specific permission to folder through AD group membership"""
        user_groups = self.get_active_ad_groups()
        folder_permissions = folder.get_permissions_by_type(permission_type)
        
        for permission in folder_permissions:
            if permission.ad_group in user_groups:
                return True
        return False
    
    def has_owned_folders(self):
        """Check if user owns any folders"""
        return len(self.owned_folders) > 0
    
    def has_validated_folders(self):
        """Check if user validates any folders"""  
        return len(self.validated_folders) > 0
    
    def has_resources_or_validations(self):
        """Check if user has any folders as owner or validator"""
        return self.has_owned_folders() or self.has_validated_folders()
    
    def to_dict(self):
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'full_name': self.full_name,
            'department': self.department,
            'is_active': self.is_active,
            'roles': [role.name for role in self.roles]
        }