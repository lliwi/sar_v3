from datetime import datetime
from app import db

# Association table for User-ADGroup many-to-many relationship
user_ad_groups = db.Table('user_ad_groups',
    db.Column('user_id', db.Integer, db.ForeignKey('users.id'), primary_key=True),
    db.Column('ad_group_id', db.Integer, db.ForeignKey('ad_groups.id'), primary_key=True),
    db.Column('created_at', db.DateTime, default=datetime.utcnow),
    db.Column('is_active', db.Boolean, default=True, nullable=False)
)

class UserADGroupMembership(db.Model):
    """Track user membership in AD groups for permission management"""
    __tablename__ = 'user_ad_group_memberships'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    ad_group_id = db.Column(db.Integer, db.ForeignKey('ad_groups.id'), nullable=False)
    granted_at = db.Column(db.DateTime, default=datetime.utcnow)
    granted_by_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    expires_at = db.Column(db.DateTime)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    notes = db.Column(db.Text)
    
    # Relationships
    user = db.relationship('User', foreign_keys=[user_id], backref='ad_group_memberships')
    ad_group = db.relationship('ADGroup', backref='user_memberships')
    granted_by = db.relationship('User', foreign_keys=[granted_by_id])
    
    # Constraints
    __table_args__ = (
        db.UniqueConstraint('user_id', 'ad_group_id', name='unique_user_ad_group'),
    )
    
    def __repr__(self):
        return f'<UserADGroupMembership {self.user.username} in {self.ad_group.name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'user': self.user.to_dict() if self.user else None,
            'ad_group': self.ad_group.to_dict() if self.ad_group else None,
            'granted_at': self.granted_at.isoformat(),
            'granted_by': self.granted_by.username if self.granted_by else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'is_active': self.is_active,
            'notes': self.notes
        }