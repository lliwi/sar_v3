from datetime import datetime
from app import db

class Role(db.Model):
    __tablename__ = 'roles'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80), unique=True, nullable=False)
    description = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<Role {self.name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description
        }
    
    @staticmethod
    def create_default_roles():
        """Create default roles if they don't exist"""
        roles = [
            {'name': 'Administrador', 'description': 'Administrador del sistema con acceso completo'},
            {'name': 'Owner', 'description': 'Propietario de recursos con permisos de validación'},
            {'name': 'Validador', 'description': 'Validador de solicitudes de permisos'},
            {'name': 'Usuario', 'description': 'Usuario estándar del sistema'}
        ]
        
        for role_data in roles:
            if not Role.query.filter_by(name=role_data['name']).first():
                role = Role(**role_data)
                db.session.add(role)
        
        db.session.commit()