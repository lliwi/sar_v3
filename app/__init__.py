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
    
    # Configure asynchronous logging with QueueHandler
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

    from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
    import queue
    import atexit

    # Create queue for async logging (non-blocking)
    log_queue = queue.Queue(-1)  # Unlimited size
    queue_handler = QueueHandler(log_queue)

    # Real handlers (executed in separate thread)
    handlers = []

    # File handler with rotation (only if directory exists and LOG_TO_FILE is enabled)
    if os.path.exists('/app/logs') and os.getenv('LOG_TO_FILE', 'true').lower() == 'true':
        file_handler = RotatingFileHandler(
            '/app/logs/app.log',
            maxBytes=int(os.getenv('LOG_MAX_BYTES', 10*1024*1024)),  # 10MB default
            backupCount=int(os.getenv('LOG_BACKUP_COUNT', 5)),        # 5 files default
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        handlers.append(file_handler)

    # Console handler (STDOUT)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level))
    handlers.append(console_handler)

    # Format: detailed for DEBUG, lightweight for production
    if log_level == 'DEBUG':
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    else:
        log_format = '%(levelname)s - %(name)s - %(message)s'  # No timestamp overhead in production

    formatter = logging.Formatter(log_format)
    for handler in handlers:
        handler.setFormatter(formatter)

    # Queue listener processes logs in background thread
    queue_listener = QueueListener(
        log_queue,
        *handlers,
        respect_handler_level=True
    )
    queue_listener.start()

    # Ensure listener stops on app shutdown
    atexit.register(queue_listener.stop)

    # Configure root logger with queue handler only
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))
    # Clear any existing handlers to avoid duplicates
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)

    # Disable propagation for all existing loggers to avoid duplicates
    for name in logging.root.manager.loggerDict:
        logger_obj = logging.getLogger(name)
        logger_obj.handlers.clear()
        logger_obj.propagate = True  # Let messages propagate to root

    # Set Flask app logger level
    app.logger.setLevel(getattr(logging, log_level))
    app.logger.handlers.clear()
    app.logger.propagate = True

    # Suppress noisy loggers in production
    if log_level != 'DEBUG':
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
        logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
        logging.getLogger('ldap3').setLevel(logging.WARNING)
    
    # Configuration - Security validation
    secret_key = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    
    # Validate SECRET_KEY in production
    if os.getenv('FLASK_ENV') == 'production' and secret_key == 'dev-secret-key-change-in-production':
        raise ValueError("❌ CRITICAL: Must set a secure SECRET_KEY in production environment!")
    
    if len(secret_key) < 32:
        app.logger.warning("⚠️  SECRET_KEY should be at least 32 characters for security")
    
    app.config['SECRET_KEY'] = secret_key
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///sar.db')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Session Security Configuration
    app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV', 'development') == 'production'
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Strict'
    app.config['PERMANENT_SESSION_LIFETIME'] = int(os.getenv('SESSION_TIMEOUT', 3600))
    app.config['WTF_CSRF_TIME_LIMIT'] = int(os.getenv('CSRF_TIMEOUT', 3600))
    
    # Celery configuration
    app.config['broker_url'] = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
    app.config['result_backend'] = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')
    
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

    # Favicon route
    @app.route('/favicon.ico')
    def favicon():
        from flask import send_from_directory
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            'favicon.ico',
            mimetype='image/vnd.microsoft.icon'
        )

    # Register timezone template filters
    from app.utils.timezone import format_local_datetime, get_timezone_name

    @app.template_filter('local_datetime')
    def local_datetime_filter(utc_datetime, format_str='%d/%m/%Y %H:%M'):
        """Filter to convert UTC datetime to local timezone"""
        return format_local_datetime(utc_datetime, format_str)

    @app.template_global()
    def get_local_timezone():
        """Template global to get timezone name"""
        return get_timezone_name()

    @app.template_filter('sanitize_paths')
    def sanitize_paths_filter(text):
        """Filter to sanitize folder paths in audit descriptions"""
        import re
        if not text:
            return text

        # Replace sequences of 3+ backslashes with 2 backslashes (UNC path normalization)
        # This handles cases like \\\\server\\path -> \\server\\path
        sanitized = re.sub(r'\\{3,}', r'\\\\', text)

        # Replace sequences of 2+ regular slashes with single slash
        sanitized = re.sub(r'/{2,}', '/', sanitized)

        return sanitized

    @app.template_filter('user_icon')
    def user_icon_filter(user):
        """Return Font Awesome icon class for user based on type (internal/external)"""
        if user.is_external_user():
            return 'fa-user-tie text-warning'
        return 'fa-user text-primary'

    @app.template_filter('user_badge_class')
    def user_badge_class_filter(user):
        """Return badge CSS class for user based on type (internal/external)"""
        if user.is_external_user():
            return 'bg-warning'
        return 'bg-primary'

    # Create tables and default data only if needed
    with app.app_context():
        # Use database lock to prevent concurrent table creation
        try:
            # Check if tables exist using SQLAlchemy inspector
            from sqlalchemy import inspect, text
            inspector = inspect(db.engine)
            existing_tables = inspector.get_table_names()

            if 'roles' not in existing_tables:
                # Try to acquire advisory lock to prevent concurrent creation
                # This ensures only one process creates tables
                with db.engine.connect() as conn:
                    # PostgreSQL advisory lock (123456789 is our app-specific lock ID)
                    lock_result = conn.execute(text("SELECT pg_try_advisory_lock(123456789)")).scalar()

                    if lock_result:
                        try:
                            # Double-check tables don't exist (another process might have created them)
                            inspector = inspect(db.engine)
                            existing_tables = inspector.get_table_names()

                            if 'roles' not in existing_tables:
                                app.logger.info("Creating database tables...")
                                db.create_all()
                                app.logger.info("Database tables created successfully")
                            else:
                                app.logger.info("Tables already exist, skipping creation")
                        finally:
                            # Release the advisory lock
                            conn.execute(text("SELECT pg_advisory_unlock(123456789)"))
                    else:
                        app.logger.info("Another process is creating tables, waiting...")
                        # Wait for the other process to finish
                        import time
                        time.sleep(2)

            # Ensure default roles exist (safe to run multiple times)
            from app.models import Role
            Role.create_default_roles()

        except Exception as e:
            app.logger.error(f"Error during database initialization: {e}")
            # Don't fail the app startup, continue anyway

        # Scheduler is now handled by a separate standalone service
        # to avoid multi-process conflicts in Gunicorn
        app.logger.info("AD Synchronization handled by standalone scheduler service")

    # Register error handlers
    register_error_handlers(app)

    return app


def register_error_handlers(app):
    """
    Register centralized error handlers for the application.
    Prevents information disclosure in production by showing generic error messages.
    """
    from flask import render_template, request

    # Support multiple debug flag formats: true/false, 1/0, yes/no
    debug_flag = os.getenv('FLASK_DEBUG', 'false').lower()
    is_debug = debug_flag in ('true', '1', 'yes')

    def wants_json():
        """Check if the request wants JSON response"""
        return request.accept_mimetypes.accept_json and \
               not request.accept_mimetypes.accept_html

    @app.errorhandler(400)
    def bad_request(error):
        """Handle 400 Bad Request errors"""
        if wants_json():
            if is_debug:
                return {'error': 'Bad Request', 'details': str(error)}, 400
            return {'error': 'Solicitud incorrecta'}, 400
        # For HTML requests, you could create a 400.html template
        return {'error': 'Solicitud incorrecta'}, 400

    @app.errorhandler(403)
    def forbidden(error):
        """Handle 403 Forbidden errors"""
        if wants_json():
            return {'error': 'Acceso denegado'}, 403
        try:
            return render_template('errors/403.html'), 403
        except:
            return {'error': 'Acceso denegado'}, 403

    @app.errorhandler(404)
    def not_found(error):
        """Handle 404 Not Found errors"""
        if wants_json():
            return {'error': 'Recurso no encontrado'}, 404
        try:
            return render_template('errors/404.html'), 404
        except:
            return {'error': 'Recurso no encontrado'}, 404

    @app.errorhandler(500)
    def internal_error(error):
        """Handle 500 Internal Server Error"""
        # Log the full error for administrators
        app.logger.error(f"Internal Server Error: {str(error)}", exc_info=True)

        if wants_json():
            if is_debug:
                return {'error': 'Error interno del servidor', 'details': str(error)}, 500
            return {'error': 'Error interno del servidor. Por favor, contacte al administrador.'}, 500

        try:
            return render_template('errors/500.html'), 500
        except:
            return {'error': 'Error interno del servidor. Por favor, contacte al administrador.'}, 500

    @app.errorhandler(Exception)
    def handle_exception(error):
        """Handle unexpected exceptions"""
        # Log the full error
        app.logger.error(f"Unhandled Exception: {str(error)}", exc_info=True)

        if wants_json():
            if is_debug:
                return {'error': 'Error inesperado', 'details': str(error), 'type': type(error).__name__}, 500
            return {'error': 'Error inesperado. Por favor, contacte al administrador.'}, 500

        try:
            return render_template('errors/500.html'), 500
        except:
            return {'error': 'Error inesperado. Por favor, contacte al administrador.'}, 500

