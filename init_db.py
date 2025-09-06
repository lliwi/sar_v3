#!/usr/bin/env python3
"""
Initialize database with default data
"""
from app import create_app, db
from app.models import Role, Folder

def init_database():
    app = create_app()
    
    with app.app_context():
        # Create all tables
        print("Creating database tables...")
        db.create_all()
        
        # Create default roles
        print("Creating default roles...")
        Role.create_default_roles()
        
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
        
        print("Database initialization complete!")

if __name__ == '__main__':
    init_database()