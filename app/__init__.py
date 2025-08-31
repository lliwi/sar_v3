from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize extensions
db = SQLAlchemy()
migrate = Migrate()
login_manager = LoginManager()
csrf = CSRFProtect()


def create_app(config_name=None):
    app = Flask(__name__)
    
    # Configure logging
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('/app/logs/app.log') if os.path.exists('/app/logs') else logging.StreamHandler()
        ]
    )
    
    # Set Flask app logger level
    app.logger.setLevel(getattr(logging, log_level))
    
    # Configuration
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///sar.db')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Celery configuration
    app.config['CELERY_BROKER_URL'] = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    app.config['CELERY_RESULT_BACKEND'] = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
    
    # SMTP configuration
    app.config['SMTP_SERVER'] = os.getenv('SMTP_SERVER')
    app.config['SMTP_PORT'] = int(os.getenv('SMTP_PORT', 587))
    app.config['SMTP_USERNAME'] = os.getenv('SMTP_USERNAME')
    app.config['SMTP_PASSWORD'] = os.getenv('SMTP_PASSWORD')
    app.config['SMTP_USE_TLS'] = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
    app.config['SMTP_FROM'] = os.getenv('SMTP_FROM', 'no-reply@playingwith.info')
    
    # Admin notifications configuration
    app.config['ADMIN_EMAIL'] = os.getenv('ADMIN_EMAIL')
    app.config['ADMIN_NOTIFICATION_ENABLED'] = os.getenv('ADMIN_NOTIFICATION_ENABLED', 'true').lower() == 'true'
    
    # LDAP configuration
    app.config['LDAP_HOST'] = os.getenv('LDAP_HOST')
    app.config['LDAP_BASE_DN'] = os.getenv('LDAP_BASE_DN')
    app.config['LDAP_GROUP_DN'] = os.getenv('LDAP_GROUP_DN')
    app.config['LDAP_BIND_USER_DN'] = os.getenv('LDAP_BIND_USER_DN')
    app.config['LDAP_BIND_USER_PASSWORD'] = os.getenv('LDAP_BIND_USER_PASSWORD')
    app.config['LDAP_ADMIN_GROUPS'] = os.getenv('LDAP_ADMIN_GROUPS', 'Domain Admins,Administrators,Enterprise Admins').split(',')
    
    # LDAP attribute mappings
    app.config['LDAP_ATTR_USER'] = os.getenv('LDAP_ATTR_USER', 'cn')
    app.config['LDAP_ATTR_DEPARTMENT'] = os.getenv('LDAP_ATTR_DEPARTMENT', 'department')
    app.config['LDAP_ATTR_EMAIL'] = os.getenv('LDAP_ATTR_EMAIL', 'mail')
    app.config['LDAP_ATTR_FIRSTNAME'] = os.getenv('LDAP_ATTR_FIRSTNAME', 'givenName')
    app.config['LDAP_ATTR_LASTNAME'] = os.getenv('LDAP_ATTR_LASTNAME', 'sn')
    
    # Multiple OU search configuration (semicolon-separated to avoid DN comma conflicts)
    app.config['LDAP_SEARCH_OUS'] = os.getenv('LDAP_SEARCH_OUS', '').split(';') if os.getenv('LDAP_SEARCH_OUS') else []
    
    # Airflow configuration
    app.config['AIRFLOW_API_URL'] = os.getenv('AIRFLOW_API_URL')
    app.config['AIRFLOW_USERNAME'] = os.getenv('AIRFLOW_USERNAME')
    app.config['AIRFLOW_PASSWORD'] = os.getenv('AIRFLOW_PASSWORD')
    app.config['AIRFLOW_AUTH_TOKEN'] = os.getenv('AIRFLOW_AUTH_TOKEN')
    app.config['AIRFLOW_DAG_NAME'] = os.getenv('AIRFLOW_DAG_NAME', 'SAR_V3')
    app.config['AIRFLOW_TIMEOUT'] = int(os.getenv('AIRFLOW_TIMEOUT', 300))
    app.config['AIRFLOW_VERIFY_SSL'] = os.getenv('AIRFLOW_VERIFY_SSL', 'false').lower() == 'true'
    app.config['AIRFLOW_RETRY_ATTEMPTS'] = int(os.getenv('AIRFLOW_RETRY_ATTEMPTS', 3))
    app.config['AIRFLOW_RETRY_DELAY'] = int(os.getenv('AIRFLOW_RETRY_DELAY', 60))
    
    # CSV configuration
    app.config['CSV_OUTPUT_DIR'] = os.getenv('CSV_OUTPUT_DIR', '/tmp/sar_csv_files')
    
    # Server URL configuration for email links
    app.config['SERVER_URL'] = os.getenv('SERVER_URL')
    app.config['BASE_URL'] = os.getenv('BASE_URL')
    
    # Initialize extensions
    db.init_app(app)
    migrate.init_app(app, db)
    csrf.init_app(app)
    
    # Configure Login Manager
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Por favor, inicia sesión para acceder a esta página.'
    login_manager.login_message_category = 'info'
    
    @login_manager.user_loader
    def load_user(user_id):
        from app.models import User
        return User.query.get(int(user_id))
    
    # Register blueprints
    from app.views.auth import auth_bp
    from app.views.main import main_bp
    from app.views.api import api_bp
    from app.views.admin import admin_bp
    
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    
    # Create tables and default data
    with app.app_context():
        db.create_all()
        from app.models import Role
        Role.create_default_roles()
    
    return app

