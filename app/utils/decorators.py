from functools import wraps
from flask import redirect, url_for, flash, abort, current_app
from flask_login import current_user
import os

def admin_required(f):
    """Decorator to require admin role"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin():
            flash('Acceso denegado. Se requieren permisos de administrador.', 'error')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated_function

def role_required(role_name):
    """Decorator to require specific role"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated or not current_user.has_role(role_name):
                flash(f'Acceso denegado. Se requiere el rol {role_name}.', 'error')
                return redirect(url_for('main.dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def owner_or_validator_required(f):
    """Decorator to require owner or validator permissions"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))

        # Admins can always access
        if current_user.is_admin():
            return f(*args, **kwargs)

        # Check if user has owned or validated folders
        if len(current_user.owned_folders) > 0 or len(current_user.validated_folders) > 0:
            return f(*args, **kwargs)

        flash('Acceso denegado. Se requieren permisos de propietario o validador.', 'error')
        return redirect(url_for('main.dashboard'))

    return decorated_function

def debug_only(f):
    """
    Decorator to restrict endpoint access to debug mode only.
    Returns 404 if debug endpoints are disabled in production.

    Usage:
        @admin_bp.route('/debug/endpoint')
        @admin_required
        @debug_only
        def debug_endpoint():
            pass
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if debug endpoints are enabled
        # Support multiple formats: true/false, 1/0, yes/no
        debug_flag = os.environ.get('ENABLE_DEBUG_ENDPOINTS', 'false').lower()
        enable_debug = debug_flag in ('true', '1', 'yes')

        if not enable_debug:
            current_app.logger.warning(
                f"Attempted access to debug endpoint {f.__name__} while debug endpoints are disabled"
            )
            abort(404)

        return f(*args, **kwargs)
    return decorated_function