#!/usr/bin/env python3
"""
Initialize database with default data
"""
from app import create_app, db
from app.models import Role, User, Folder, ADGroup

def init_database():
    app = create_app()
    
    with app.app_context():
        # Create all tables
        print("Creating database tables...")
        db.create_all()
        
        # Create default roles
        print("Creating default roles...")
        Role.create_default_roles()
        
        # Create admin user (optional, for development)
        admin_role = Role.query.filter_by(name='Administrador').first()
        if admin_role and not User.query.filter_by(username='admin').first():
            admin_user = User(
                username='admin',
                email='admin@company.com',
                full_name='Administrador del Sistema',
                department='IT',
                is_active=True
            )
            admin_user.roles = [admin_role]
            db.session.add(admin_user)
            db.session.commit()
            print("Created admin user (username: admin)")
        
        # Create sample folder (optional, for development)
        if not Folder.query.first():
            sample_folder = Folder(
                name='Carpeta de Ejemplo',
                path='\\\\server\\shared\\example',
                description='Carpeta de ejemplo para pruebas',
                is_active=True
            )
            db.session.add(sample_folder)
            db.session.commit()
            print("Created sample folder")
        
        # Create sample AD group (optional, for development)
        if not ADGroup.query.first():
            sample_group = ADGroup(
                name='DOMAIN_USERS',
                distinguished_name='CN=Domain Users,CN=Users,DC=company,DC=com',
                description='Usuarios del dominio',
                group_type='Security',
                is_active=True
            )
            db.session.add(sample_group)
            db.session.commit()
            print("Created sample AD group")
        
        print("Database initialization complete!")

if __name__ == '__main__':
    init_database()