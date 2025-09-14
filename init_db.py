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

        # Apply any pending migrations to ensure schema is up to date
        print("Applying database migrations...")
        from flask_migrate import upgrade
        upgrade()

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

        # Verify that the permission_requests table has the correct constraints
        print("Verifying database schema...")
        try:
            from sqlalchemy import text
            result = db.session.execute(text("""
                SELECT conname
                FROM pg_constraint
                WHERE conname = 'check_request_status'
                AND conrelid = 'permission_requests'::regclass
            """))
            constraint = result.fetchone()

            if constraint:
                print("✓ Permission request status constraint verified")
            else:
                print("⚠ Warning: Permission request status constraint not found")

        except Exception as e:
            print(f"⚠ Warning: Could not verify database constraints: {e}")

        print("Database initialization complete!")

if __name__ == '__main__':
    init_database()