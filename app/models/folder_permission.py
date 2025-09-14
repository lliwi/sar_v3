from datetime import datetime
from app import db

class FolderPermission(db.Model):
    __tablename__ = 'folder_permissions'
    
    id = db.Column(db.Integer, primary_key=True)
    folder_id = db.Column(db.Integer, db.ForeignKey('folders.id'), nullable=False)
    ad_group_id = db.Column(db.Integer, db.ForeignKey('ad_groups.id'), nullable=False)
    permission_type = db.Column(db.String(20), nullable=False)  # 'read', 'write'
    granted_by_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    granted_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    deletion_in_progress = db.Column(db.Boolean, default=False, nullable=False)
    
    # Relationships
    granted_by = db.relationship('User', backref='granted_permissions')
    
    # Constraints
    __table_args__ = (
        db.UniqueConstraint('folder_id', 'ad_group_id', 'permission_type', name='unique_folder_group_permission'),
        db.CheckConstraint(permission_type.in_(['read', 'write']), name='check_permission_type')
    )
    
    def __repr__(self):
        return f'<FolderPermission {self.folder.path} - {self.ad_group.name} - {self.permission_type}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'folder_id': self.folder_id,
            'folder_path': self.folder.path if self.folder else None,
            'ad_group_id': self.ad_group_id,
            'ad_group_name': self.ad_group.name if self.ad_group else None,
            'permission_type': self.permission_type,
            'granted_by': self.granted_by.username if self.granted_by else None,
            'granted_at': self.granted_at.isoformat(),
            'is_active': self.is_active,
            'deletion_in_progress': self.deletion_in_progress
        }