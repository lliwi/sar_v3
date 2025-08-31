from functools import wraps
from flask import redirect, url_for, flash
from flask_login import current_user

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