from datetime import datetime
from app import db

class UserFolderPermission(db.Model):
    """Direct user permissions to folders (separate from AD group permissions)"""
    __tablename__ = 'user_folder_permissions'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    folder_id = db.Column(db.Integer, db.ForeignKey('folders.id'), nullable=False)
    permission_type = db.Column(db.String(20), nullable=False)  # 'read', 'write'
    granted_by_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    granted_at = db.Column(db.DateTime, default=datetime.utcnow)
    expires_at = db.Column(db.DateTime)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    notes = db.Column(db.Text)
    
    # Relationships
    user = db.relationship('User', foreign_keys=[user_id], backref='direct_folder_permissions')
    folder = db.relationship('Folder', backref='direct_user_permissions')
    granted_by = db.relationship('User', foreign_keys=[granted_by_id])
    
    # Constraints
    __table_args__ = (
        db.UniqueConstraint('user_id', 'folder_id', 'permission_type', name='unique_user_folder_permission'),
        db.CheckConstraint(permission_type.in_(['read', 'write']), name='check_user_permission_type')
    )
    
    def __repr__(self):
        return f'<UserFolderPermission {self.user.username} - {self.folder.path} - {self.permission_type}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'user': self.user.to_dict() if self.user else None,
            'folder': {
                'id': self.folder.id,
                'name': self.folder.name,
                'path': self.folder.path
            } if self.folder else None,
            'permission_type': self.permission_type,
            'granted_by': self.granted_by.username if self.granted_by else None,
            'granted_at': self.granted_at.isoformat(),
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'is_active': self.is_active,
            'notes': self.notes
        }