#!/usr/bin/env python3
"""
Initialize database with default data
"""
from app import create_app, db
from app.models import Role, Folder

def init_database():
    app = create_app()

    with app.app_context():
        # For new installations, create all tables directly from models
        print("Creating database tables...")
        db.create_all()

        # Skip migrations for new installations and mark them as applied
        print("Initializing migration tracking for new installation...")
        try:
            from flask_migrate import stamp
            from sqlalchemy import text

            # Check if alembic_version table exists (indicates existing installation)
            result = db.session.execute(text("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = 'alembic_version'
            """))

            has_alembic = result.scalar() > 0

            if not has_alembic:
                # New installation - mark all migrations as applied
                stamp()
                print("✓ New installation: All migrations marked as applied")
            else:
                # Existing installation - try to apply pending migrations carefully
                print("Existing installation detected, applying pending migrations...")
                try:
                    from flask_migrate import upgrade
                    upgrade()
                    print("✓ Migrations applied successfully")
                except Exception as migration_error:
                    print(f"⚠ Migration warning: {migration_error}")
                    print("Database may already be up to date")

        except Exception as e:
            print(f"⚠ Warning: Could not handle migrations: {e}")
            print("Continuing with database initialization...")

        # Ensure deletion_in_progress column exists in folder_permissions table
        print("Verifying folder_permissions table schema...")
        try:
            from sqlalchemy import text

            # First check if folder_permissions table exists
            table_check = db.session.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name = 'folder_permissions'
            """))
            table_exists = table_check.fetchone()

            if table_exists:
                # Check if deletion_in_progress column exists
                result = db.session.execute(text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'folder_permissions'
                    AND column_name = 'deletion_in_progress'
                """))
                column_exists = result.fetchone()

                if not column_exists:
                    print("Adding deletion_in_progress column to folder_permissions table...")
                    db.session.execute(text("""
                        ALTER TABLE folder_permissions
                        ADD COLUMN deletion_in_progress BOOLEAN NOT NULL DEFAULT FALSE
                    """))
                    db.session.commit()
                    print("✓ deletion_in_progress column added successfully")
                else:
                    print("✓ deletion_in_progress column already exists")
            else:
                print("ℹ folder_permissions table will be created by db.create_all() with all fields")

        except Exception as e:
            print(f"⚠ Warning: Could not verify/add deletion_in_progress column: {e}")

        # Ensure acknowledge columns exist in users table
        print("Verifying users table acknowledge schema...")
        try:
            from sqlalchemy import text

            # First check if users table exists
            table_check = db.session.execute(text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name = 'users'
            """))
            table_exists = table_check.fetchone()

            if table_exists:
                acknowledge_columns = ['ad_acknowledged', 'ad_acknowledged_at', 'ad_acknowledged_by']

                for column in acknowledge_columns:
                    # Check if column exists
                    result = db.session.execute(text(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'users'
                        AND column_name = '{column}'
                    """))
                    column_exists = result.fetchone()

                    if not column_exists:
                        print(f"Adding {column} column to users table...")
                        if column == 'ad_acknowledged':
                            db.session.execute(text("""
                                ALTER TABLE users
                                ADD COLUMN ad_acknowledged BOOLEAN NOT NULL DEFAULT FALSE
                            """))
                        elif column == 'ad_acknowledged_at':
                            db.session.execute(text("""
                                ALTER TABLE users
                                ADD COLUMN ad_acknowledged_at TIMESTAMP
                            """))
                        elif column == 'ad_acknowledged_by':
                            db.session.execute(text("""
                                ALTER TABLE users
                                ADD COLUMN ad_acknowledged_by INTEGER
                            """))
                        print(f"✓ {column} column added successfully")
                    else:
                        print(f"✓ {column} column already exists")

                # Add foreign key constraint for ad_acknowledged_by if it doesn't exist
                try:
                    fk_check = db.session.execute(text("""
                        SELECT conname
                        FROM pg_constraint
                        WHERE conname = 'fk_users_ad_acknowledged_by'
                        AND conrelid = 'users'::regclass
                    """))
                    fk_exists = fk_check.fetchone()

                    if not fk_exists:
                        print("Adding foreign key constraint for ad_acknowledged_by...")
                        db.session.execute(text("""
                            ALTER TABLE users
                            ADD CONSTRAINT fk_users_ad_acknowledged_by
                            FOREIGN KEY (ad_acknowledged_by) REFERENCES users(id) ON DELETE SET NULL
                        """))
                        print("✓ Foreign key constraint added successfully")
                    else:
                        print("✓ Foreign key constraint already exists")
                except Exception as fk_error:
                    print(f"⚠ Warning: Could not add foreign key constraint: {fk_error}")

                db.session.commit()
            else:
                print("ℹ users table will be created by db.create_all() with all fields")

        except Exception as e:
            print(f"⚠ Warning: Could not verify/add acknowledge columns: {e}")

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