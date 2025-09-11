from flask import Blueprint, render_template, request, flash, redirect, url_for
from flask_login import login_user, logout_user, login_required, current_user
from app.forms import LoginForm
from app.models import User, AuditEvent
from app.services.ldap_service import LDAPService
from app import db
import logging
import traceback

auth_bp = Blueprint('auth', __name__)

logger = logging.getLogger(__name__)

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    
    form = LoginForm()
    if form.validate_on_submit():
        username = form.username.data.lower()
        password = form.password.data
        
        try:
            logger.info(f"Attempting login for user: {username}")
            
            # Try LDAP authentication
            ldap_service = LDAPService()
            logger.debug(f"LDAP service initialized. Host: {ldap_service.host}, Search OUs: {ldap_service.search_ous}")
            
            user_data = ldap_service.authenticate_user(username, password)
            logger.debug(f"LDAP authentication result for {username}: {'Success' if user_data else 'Failed'}")
            
            if user_data:
                logger.info(f"LDAP authentication successful for {username}")
                logger.debug(f"LDAP user data for {username}: {user_data}")
            else:
                logger.warning(f"LDAP authentication failed for {username}")
                
        except Exception as e:
            logger.error(f"Exception during LDAP authentication for {username}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Log audit event for system error
            try:
                AuditEvent.log_event(
                    user=None,
                    event_type='login',
                    action='error',
                    description=f'Error interno durante login de {username}: {str(e)}',
                    metadata={'username': username, 'error': str(e)},
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
            except Exception as audit_error:
                logger.error(f"Failed to log audit event: {audit_error}")
            
            flash('Error interno del servidor. Por favor, contacte al administrador.', 'error')
            return render_template('auth/login.html', title='Iniciar Sesión', form=form)
        
        if user_data:
            try:
                logger.info(f"Processing user data for {username}")
                
                # Find or create user in database
                email = user_data.get('email', f"{username}@company.com")
                
                # First try to find by username
                user = User.query.filter_by(username=username).first()
                
                # If not found by username, try to find by email to avoid uniqueness violations
                if not user:
                    user = User.query.filter_by(email=email).first()
                    if user:
                        # User exists with this email but different username - update username
                        logger.info(f"Found user by email {email}, updating username to {username}")
                        user.username = username
                
                if not user:
                    logger.info(f"Creating new user record for {username}")
                    try:
                        user = User(
                            username=username,
                            email=email,
                            full_name=user_data.get('full_name', username),
                            department=user_data.get('department')
                        )
                        db.session.add(user)
                        db.session.flush()  # Flush to catch integrity errors before commit
                        logger.info(f"New user record created for {username}")
                    except Exception as create_error:
                        logger.error(f"Error creating user {username}: {create_error}")
                        db.session.rollback()
                        # Try to find the user again in case of race condition
                        user = User.query.filter_by(username=username).first()
                        if not user:
                            user = User.query.filter_by(email=email).first()
                        if not user:
                            raise create_error
                
                # Update user information from LDAP
                logger.debug(f"Updating user information for {username}")
                user.email = user_data.get('email', user.email)
                user.full_name = user_data.get('full_name', user.full_name)
                user.department = user_data.get('department', user.department)
                user.last_login = db.func.now()
                
                # Auto-assign roles based on AD groups
                from app.models import Role
                from flask import current_app
                user_groups = user_data.get('groups', [])
                
                # Get admin groups from configuration
                admin_groups = current_app.config.get('LDAP_ADMIN_GROUPS', ['Domain Admins', 'Administrators', 'Enterprise Admins'])
                # Strip whitespace from group names
                admin_groups = [group.strip() for group in admin_groups]
                
                # Check if user is in admin groups
                # Extract group names from DNs (format: CN=GroupName,OU=...)
                user_group_names = []
                for group_dn in user_groups:
                    if group_dn.startswith('CN='):
                        # Extract group name from DN
                        group_name = group_dn.split(',')[0][3:]  # Remove 'CN=' prefix
                        user_group_names.append(group_name)
                
                is_domain_admin = any(group_name in admin_groups for group_name in user_group_names)
                
                # Log group info at INFO level only for admin users or essential security events
                if is_domain_admin:
                    logger.info(f"Admin user {username} logged in - groups: {len(user_groups)} total")
                else:
                    logger.debug(f"User {username} group membership processed - {len(user_groups)} groups")
                
                if is_domain_admin:
                    admin_role = Role.query.filter_by(name='Administrador').first()
                    if admin_role and admin_role not in user.roles:
                        user.roles.append(admin_role)
                        logger.info(f"Admin role assigned to user {username}")
                
                db.session.commit()
                logger.info(f"User data committed to database for {username}")
                
                # Prevent session fixation by regenerating session ID
                from flask import session
                session.permanent = True
                session.regenerate = True  # Mark for regeneration
                
                # Log the user in
                login_user(user, remember=form.remember_me.data)
                logger.info(f"User {username} logged in successfully")
                
                # Log audit event
                AuditEvent.log_event(
                    user=user,
                    event_type='login',
                    action='success',
                    description=f'Usuario {username} inició sesión exitosamente',
                    ip_address=request.remote_addr,
                    user_agent=request.headers.get('User-Agent')
                )
                
                flash('¡Bienvenido! Has iniciado sesión correctamente.', 'success')
                
                # Redirect to next page or dashboard
                next_page = request.args.get('next')
                return redirect(next_page) if next_page else redirect(url_for('main.dashboard'))
                
            except Exception as e:
                logger.error(f"Exception during user processing for {username}: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                
                # Rollback any database changes
                try:
                    db.session.rollback()
                except Exception as rollback_error:
                    logger.error(f"Failed to rollback database session: {rollback_error}")
                
                # Log audit event for system error
                try:
                    AuditEvent.log_event(
                        user=None,
                        event_type='login',
                        action='error',
                        description=f'Error procesando usuario {username}: {str(e)}',
                        metadata={'username': username, 'error': str(e)},
                        ip_address=request.remote_addr,
                        user_agent=request.headers.get('User-Agent')
                    )
                except Exception as audit_error:
                    logger.error(f"Failed to log audit event: {audit_error}")
                
                flash('Error interno procesando el usuario. Por favor, contacte al administrador.', 'error')
                return render_template('auth/login.html', title='Iniciar Sesión', form=form)
        else:
            # Log failed authentication
            AuditEvent.log_event(
                user=None,
                event_type='login',
                action='failed',
                description=f'Intento de login fallido para usuario {username}',
                metadata={'username': username},
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            flash('Usuario o contraseña incorrectos.', 'error')
    
    return render_template('auth/login.html', title='Iniciar Sesión', form=form)

@auth_bp.route('/logout')
@login_required
def logout():
    # Log audit event
    AuditEvent.log_event(
        user=current_user,
        event_type='logout',
        action='success',
        description=f'Usuario {current_user.username} cerró sesión',
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    # Clear session completely to prevent session reuse
    from flask import session
    session.clear()
    
    logout_user()
    flash('Has cerrado sesión correctamente.', 'info')
    return redirect(url_for('auth.login'))