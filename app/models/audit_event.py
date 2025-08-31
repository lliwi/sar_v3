from datetime import datetime
import json
from app import db

class AuditEvent(db.Model):
    __tablename__ = 'audit_events'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    event_type = db.Column(db.String(50), nullable=False, index=True)  # 'login', 'permission_request', 'permission_granted', etc.
    resource_type = db.Column(db.String(50))  # 'folder', 'user', 'permission', etc.
    resource_id = db.Column(db.Integer)
    action = db.Column(db.String(50), nullable=False)  # 'create', 'update', 'delete', 'approve', 'reject'
    description = db.Column(db.Text)
    event_data = db.Column(db.Text)  # JSON string with additional data
    ip_address = db.Column(db.String(45))  # IPv4 or IPv6
    user_agent = db.Column(db.String(500))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    
    # Relationships
    user = db.relationship('User', backref='audit_events')
    
    def __repr__(self):
        return f'<AuditEvent {self.event_type} - {self.action} by {self.user.username if self.user else "System"}>'
    
    def set_metadata(self, data):
        """Set metadata as JSON string"""
        if data:
            self.event_data = json.dumps(data)
    
    def get_metadata(self):
        """Get metadata as Python object"""
        if self.event_data:
            try:
                return json.loads(self.event_data)
            except json.JSONDecodeError:
                return {}
        return {}
    
    def to_dict(self):
        return {
            'id': self.id,
            'user': self.user.username if self.user else 'System',
            'user_name': self.user.full_name if self.user else 'System',
            'event_type': self.event_type,
            'resource_type': self.resource_type,
            'resource_id': self.resource_id,
            'action': self.action,
            'description': self.description,
            'metadata': self.get_metadata(),
            'ip_address': self.ip_address,
            'user_agent': self.user_agent,
            'created_at': self.created_at.isoformat()
        }
    
    @staticmethod
    def log_event(user, event_type, action, description=None, resource_type=None, 
                  resource_id=None, metadata=None, ip_address=None, user_agent=None):
        """Create and save an audit event"""
        event = AuditEvent(
            user=user,
            event_type=event_type,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            description=description,
            ip_address=ip_address,
            user_agent=user_agent
        )
        
        if metadata:
            event.set_metadata(metadata)
        
        db.session.add(event)
        db.session.commit()
        
        return event