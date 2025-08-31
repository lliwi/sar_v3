from datetime import datetime
from app import db

class ADGroup(db.Model):
    __tablename__ = 'ad_groups'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), unique=True, nullable=False, index=True)
    distinguished_name = db.Column(db.String(500), unique=True, nullable=False)
    description = db.Column(db.Text)
    group_type = db.Column(db.String(50))  # Security, Distribution, etc.
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    last_sync = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    permissions = db.relationship('FolderPermission', backref='ad_group', lazy=True, cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<ADGroup {self.name}>'
    
    def get_folders_with_permission(self, permission_type):
        """Get all folders where this group has specific permission type"""
        return [fp.folder for fp in self.permissions if fp.permission_type == permission_type]
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'distinguished_name': self.distinguished_name,
            'description': self.description,
            'group_type': self.group_type,
            'is_active': self.is_active,
            'last_sync': self.last_sync.isoformat() if self.last_sync else None
        }