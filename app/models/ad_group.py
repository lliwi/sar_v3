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

    # AD Status tracking (similar to User model)
    ad_status = db.Column(db.String(20), default='active', nullable=False)  # active, not_found, error, disabled
    ad_last_check = db.Column(db.DateTime)
    ad_error_count = db.Column(db.Integer, default=0)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    permissions = db.relationship('FolderPermission', backref='ad_group', lazy=True, cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<ADGroup {self.name}>'
    
    def get_folders_with_permission(self, permission_type):
        """Get all folders where this group has specific permission type"""
        return [fp.folder for fp in self.permissions if fp.permission_type == permission_type]
    
    def get_ad_status_display(self):
        """Get a human-readable display of the AD status"""
        status_map = {
            'active': 'Activo en AD',
            'not_found': 'No encontrado en AD',
            'error': 'Error de verificación',
            'disabled': 'Deshabilitado en AD'
        }
        return status_map.get(self.ad_status, self.ad_status)

    def mark_ad_active(self):
        """Mark group as active in AD"""
        self.ad_status = 'active'
        self.ad_last_check = datetime.utcnow()
        self.ad_error_count = 0

    def mark_ad_not_found(self):
        """Mark group as not found in AD"""
        self.ad_status = 'not_found'
        self.ad_last_check = datetime.utcnow()
        self.ad_error_count = (self.ad_error_count or 0) + 1

    def mark_ad_error(self):
        """Mark group as having AD verification error"""
        self.ad_status = 'error'
        self.ad_last_check = datetime.utcnow()
        self.ad_error_count = (self.ad_error_count or 0) + 1

    def mark_ad_disabled(self):
        """Mark group as disabled in AD"""
        self.ad_status = 'disabled'
        self.ad_last_check = datetime.utcnow()
        self.ad_error_count = (self.ad_error_count or 0) + 1

    def get_affected_folders(self):
        """Get folders that would be affected if this group has problems"""
        return [fp.folder for fp in self.permissions if fp.is_active]

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'distinguished_name': self.distinguished_name,
            'description': self.description,
            'group_type': self.group_type,
            'is_active': self.is_active,
            'last_sync': self.last_sync.isoformat() if self.last_sync else None,
            'ad_status': self.ad_status,
            'ad_status_display': self.get_ad_status_display(),
            'ad_last_check': self.ad_last_check.isoformat() if self.ad_last_check else None,
            'ad_error_count': self.ad_error_count or 0
        }