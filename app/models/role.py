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

        try:
            for role_data in roles:
                existing_role = Role.query.filter_by(name=role_data['name']).first()
                if not existing_role:
                    role = Role(**role_data)
                    db.session.add(role)

            if db.session.new:  # Only commit if there are new objects
                db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"Error creating default roles: {e}")
            raise