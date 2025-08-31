from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy.orm import joinedload
from datetime import datetime
from app.models import PermissionRequest, Folder, AuditEvent, FolderPermission, User, UserFolderPermission
from app.forms import PermissionRequestForm, PermissionValidationForm
from app.services.email_service import send_permission_request_notification
from app.services.airflow_service import trigger_permission_changes
from app import db

main_bp = Blueprint('main', __name__)

@main_bp.route('/health')
def health_check():
    """Health check endpoint for Docker"""
    health_status = {
        'status': 'healthy',
        'message': 'Application is running',
        'checks': {}
    }
    
    try:
        # Database connectivity check
        from app import db
        from sqlalchemy import text
        with db.engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        health_status['checks']['database'] = 'ok'
    except Exception as e:
        health_status['checks']['database'] = f'error: {str(e)}'
        health_status['status'] = 'degraded'
    
    try:
        # LDAP connectivity check (non-critical)
        from app.services.ldap_service import LDAPService
        import os
        if os.getenv('LDAP_HOST'):
            ldap_service = LDAPService()
            # Just check if we can create the service, don't actually connect
            health_status['checks']['ldap'] = 'configured'
        else:
            health_status['checks']['ldap'] = 'not_configured'
    except Exception as e:
        # LDAP errors are non-critical for basic health
        health_status['checks']['ldap'] = f'error: {str(e)}'
    
    # Return healthy if database is ok, even if LDAP has issues
    status_code = 200 if health_status['checks'].get('database') == 'ok' else 500
    
    return jsonify(health_status), status_code

@main_bp.route('/')
@login_required
def dashboard():
    # Get user's pending requests
    pending_requests = PermissionRequest.query.filter_by(
        requester=current_user,
        status='pending'
    ).order_by(PermissionRequest.created_at.desc()).limit(5).all()
    
    # Get requests pending validation by current user
    validation_requests = []
    if current_user.is_admin():
        validation_requests = PermissionRequest.query.filter_by(status='pending').limit(5).all()
    else:
        # Non-admin users see ONLY requests specifically assigned to them as validator
        # This matches the filtering logic used in pending_validations()
        validation_requests = PermissionRequest.query.filter_by(
            status='pending',
            validator_id=current_user.id
        ).limit(5).all()
    
    # Get recent audit events
    recent_events = AuditEvent.query.filter_by(user=current_user).order_by(
        AuditEvent.created_at.desc()
    ).limit(5).all()
    
    stats = {
        'pending_requests': len(pending_requests),
        'validation_requests': len(validation_requests),
        'total_folders': Folder.query.filter_by(is_active=True).count(),
        'user_permissions': FolderPermission.query.join(Folder).filter(
            FolderPermission.is_active == True,
            Folder.owners.contains(current_user)
        ).count()
    }
    
    # Add task statistics for administrators
    if current_user.is_admin():
        from app.models import Task
        stats.update({
            'total_tasks': Task.query.count(),
            'pending_tasks': Task.query.filter_by(status='pending').count(),
            'running_tasks': Task.query.filter_by(status='running').count(),
            'failed_tasks': Task.query.filter_by(status='failed').count(),
            'completed_tasks': Task.query.filter_by(status='completed').count()
        })
    
    return render_template('main/dashboard.html', 
                         pending_requests=pending_requests,
                         validation_requests=validation_requests,
                         recent_events=recent_events,
                         stats=stats)

@main_bp.route('/request-permission', methods=['GET', 'POST'])
@login_required
def request_permission():
    form = PermissionRequestForm()
    
    # Both folder_id and validator_id are now IntegerFields with custom validation
    # No need to populate choices - the custom comboboxes handle the UI
    
    if form.validate_on_submit():
        # Check for existing permissions (manual and AD-sync)
        existing_permission_check = PermissionRequest.check_existing_permissions(
            current_user.id, 
            form.folder_id.data, 
            form.permission_type.data
        )
        
        # Handle different scenarios
        if existing_permission_check['action'] == 'error':
            flash(existing_permission_check['message'], 'error')
            return redirect(url_for('main.request_permission'))
        
        elif existing_permission_check['action'] == 'duplicate':
            # Permission already exists - warn user and don't proceed
            flash(existing_permission_check['message'], 'warning')
            return redirect(url_for('main.request_permission'))
        
        elif existing_permission_check['action'] == 'change':
            # Different permission type exists - create change request
            
            # Cancel any pending request first
            if existing_permission_check.get('existing_source') == 'pending':
                existing_request = existing_permission_check.get('existing_request')
                if existing_request:
                    existing_request.cancel(current_user, "Cancelada para cambio de tipo de permiso")
                    db.session.commit()
            
            # Create permission change request
            permission_request = PermissionRequest.create_permission_change_request(
                requester=current_user,
                folder_id=form.folder_id.data,
                validator_id=form.validator_id.data,
                new_permission_type=form.permission_type.data,
                business_need=form.business_need.data,
                existing_permission_info=existing_permission_check
            )
            
            # Check if there are applicable groups for the new permission type
            applicable_groups = permission_request.get_applicable_groups()
            if not applicable_groups:
                flash(f'No hay grupos configurados para permisos de {form.permission_type.data} en esta carpeta. Contacte al administrador.', 'error')
                return redirect(url_for('main.request_permission'))
            
            # Assign groups automatically before saving
            permission_request.assign_groups_automatically()
            
            db.session.add(permission_request)
            db.session.commit()
            
            # Log audit event for permission change
            AuditEvent.log_event(
                user=current_user,
                event_type='permission_change_request',
                action='create',
                resource_type='permission_request',
                resource_id=permission_request.id,
                description=f'Solicitud de cambio de permiso: {existing_permission_check["existing_permission_type"]} → {form.permission_type.data} para carpeta {permission_request.folder.path}',
                metadata={
                    'folder_path': permission_request.folder.path,
                    'old_permission_type': existing_permission_check['existing_permission_type'],
                    'new_permission_type': form.permission_type.data,
                    'existing_source': existing_permission_check['existing_source'],
                    'applicable_groups': [g.name for g in applicable_groups],
                    'is_change_request': True
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            # Send notification email
            send_permission_request_notification(permission_request.id)
            
            flash(f'Solicitud de cambio enviada: {existing_permission_check["existing_permission_type"]} → {form.permission_type.data}. Se crearán tareas para eliminar el permiso actual y aplicar el nuevo.', 'info')
            return redirect(url_for('main.dashboard'))
        
        else:  # action == 'new'
            # Standard new permission request
            permission_request = PermissionRequest(
                requester=current_user,
                folder_id=form.folder_id.data,
                validator_id=form.validator_id.data,
                permission_type=form.permission_type.data,
                justification=form.business_need.data,
                business_need=form.business_need.data,
                expires_at=None
            )
            
            # Check if there are applicable groups for this folder and permission type
            applicable_groups = permission_request.get_applicable_groups()
            if not applicable_groups:
                flash(f'No hay grupos configurados para permisos de {form.permission_type.data} en esta carpeta. Contacte al administrador.', 'error')
                return redirect(url_for('main.request_permission'))
            
            # Assign groups automatically before saving
            permission_request.assign_groups_automatically()
            
            db.session.add(permission_request)
            db.session.commit()
            
            # Log audit event
            AuditEvent.log_event(
                user=current_user,
                event_type='permission_request',
                action='create',
                resource_type='permission_request',
                resource_id=permission_request.id,
                description=f'Solicitud de permiso {form.permission_type.data} para carpeta {permission_request.folder.path}',
                metadata={
                    'folder_path': permission_request.folder.path,
                    'folder_description': permission_request.folder.description,
                    'applicable_groups': [g.name for g in applicable_groups],
                    'permission_type': form.permission_type.data
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            # Send notification email
            send_permission_request_notification(permission_request.id)
            
            flash('Tu solicitud de permiso ha sido enviada y está pendiente de validación.', 'success')
            return redirect(url_for('main.dashboard'))
    
    return render_template('main/request_permission.html', title='Solicitar Permiso', form=form)


@main_bp.route('/my-requests')
@login_required
def my_requests():
    page = request.args.get('page', 1, type=int)
    requests = PermissionRequest.query.filter_by(requester=current_user).order_by(
        PermissionRequest.created_at.desc()
    ).paginate(page=page, per_page=20, error_out=False)
    
    return render_template('main/my_requests.html', 
                         title='Mis Solicitudes',
                         requests=requests)

@main_bp.route('/my-permissions')
@login_required
def my_permissions():
    """Show only folders where the user has real access permissions through AD groups"""
    page = request.args.get('page', 1, type=int)
    
    # Get approved permission requests for the current user
    approved_requests = PermissionRequest.query.filter_by(
        requester=current_user,
        status='approved'
    ).options(
        db.joinedload(PermissionRequest.folder),
        db.joinedload(PermissionRequest.ad_group)
    ).order_by(PermissionRequest.validation_date.desc()).paginate(
        page=page, per_page=20, error_out=False
    )
    
    try:
        # Get current user's AD groups
        from app.services.ldap_service import LDAPService
        ldap_service = LDAPService()
        user_groups = ldap_service.get_user_groups(current_user.username)
        
        if not user_groups:
            # If no groups found, show empty result
            return render_template('main/my_permissions.html',
                                 title='Mis Permisos',
                                 approved_requests=approved_requests,
                                 permissions_by_folder={})
        
        # Get AD group names (extract CN from full DN)
        user_group_names = []
        for group in user_groups:
            # Extract CN from LDAP DN format: CN=groupname,OU=...
            if group.startswith('CN='):
                group_name = group.split(',')[0].replace('CN=', '')
                user_group_names.append(group_name)
            else:
                user_group_names.append(group)
        
        # Find folders where user has access through their AD groups
        from app.models import ADGroup
        accessible_folders = []
        permissions_by_folder = {}
        
        # Get all folder permissions for groups the user belongs to
        user_ad_groups = ADGroup.query.filter(ADGroup.name.in_(user_group_names)).all()
        
        if user_ad_groups:
            folder_permissions = FolderPermission.query.filter(
                FolderPermission.ad_group_id.in_([g.id for g in user_ad_groups]),
                FolderPermission.is_active == True
            ).options(
                db.joinedload(FolderPermission.folder),
                db.joinedload(FolderPermission.ad_group)
            ).all()
            
            # Organize permissions by folder
            for permission in folder_permissions:
                folder_id = permission.folder_id
                folder = permission.folder
                
                # Only show active folders
                if not folder.is_active:
                    continue
                    
                if folder_id not in permissions_by_folder:
                    permissions_by_folder[folder_id] = {
                        'folder': folder,
                        'read_groups': [],
                        'write_groups': []
                    }
                    if folder not in accessible_folders:
                        accessible_folders.append(folder)
                
                # Add the group to the appropriate permission type
                if permission.permission_type == 'read':
                    permissions_by_folder[folder_id]['read_groups'].append(permission.ad_group)
                elif permission.permission_type == 'write':
                    permissions_by_folder[folder_id]['write_groups'].append(permission.ad_group)
        
    except Exception as e:
        # If LDAP is not available or there's an error, fall back to empty result
        import logging
        logging.error(f"Error getting user groups for {current_user.username}: {e}")
        permissions_by_folder = {}
    
    return render_template('main/my_permissions.html',
                         title='Mis Permisos',
                         approved_requests=approved_requests,
                         permissions_by_folder=permissions_by_folder)

@main_bp.route('/pending-validations')
@login_required
def pending_validations():
    page = request.args.get('page', 1, type=int)
    
    if current_user.is_admin():
        # Admins see all pending requests
        requests = PermissionRequest.query.filter_by(status='pending')
    else:
        # Non-admin users see ONLY requests specifically assigned to them as validator
        # This means they only see requests where validator_id matches their user ID
        requests = PermissionRequest.query.filter_by(
            status='pending',
            validator_id=current_user.id
        )
    
    # Load all necessary relationships to avoid N+1 queries
    requests = requests.options(
        db.joinedload(PermissionRequest.requester),
        db.joinedload(PermissionRequest.validator),
        db.joinedload(PermissionRequest.folder),
        db.joinedload(PermissionRequest.ad_group)
    ).order_by(PermissionRequest.created_at.desc()).paginate(
        page=page, per_page=20, error_out=False
    )
    
    return render_template('main/pending_validations.html', 
                         title='Validaciones Pendientes',
                         requests=requests)

@main_bp.route('/my-resources')
@login_required
def my_resources():
    """Show folders that the current user owns or validates"""
    page = request.args.get('page', 1, type=int)
    folder_filter = request.args.get('filter', '').strip()
    
    # Get folders where the user is owner or validator
    owned_folders = current_user.owned_folders
    validated_folders = current_user.validated_folders
    
    # Combine and remove duplicates while preserving folder objects
    all_managed_folders = list(owned_folders)
    for folder in validated_folders:
        if folder not in all_managed_folders:
            all_managed_folders.append(folder)
    
    # Filter only active folders
    managed_folders = [f for f in all_managed_folders if f.is_active]
    
    # Apply name filter if provided
    if folder_filter:
        filtered_folders = []
        for f in managed_folders:
            matches = folder_filter.lower() in f.name.lower() or folder_filter.lower() in f.path.lower()
            if matches:
                filtered_folders.append(f)
        managed_folders = filtered_folders
        print(f"DEBUG: After filtering: {len(managed_folders)} folders remain")
    
    # Sort by name
    managed_folders.sort(key=lambda x: x.name.lower())
    
    # Get folder details with permissions
    folder_details = []
    for folder in managed_folders:
        # Determine user's role for this folder
        is_owner = folder in owned_folders
        is_validator = folder in validated_folders
        role = []
        if is_owner:
            role.append('Propietario')
        if is_validator:
            role.append('Validador')
        
        # Get folder permissions by type
        read_permissions = folder.get_permissions_by_type('read')
        write_permissions = folder.get_permissions_by_type('write')
        
        folder_details.append({
            'folder': folder,
            'role': ' / '.join(role),
            'is_owner': is_owner,
            'is_validator': is_validator,
            'read_permissions': read_permissions,
            'write_permissions': write_permissions,
            'total_permissions': len(read_permissions) + len(write_permissions)
        })
    
    # Paginate results manually since we're working with a list
    per_page = 10
    total = len(folder_details)
    start = (page - 1) * per_page
    end = start + per_page
    paginated_folders = folder_details[start:end]
    
    # Create pagination info
    pagination_info = {
        'page': page,
        'per_page': per_page,
        'total': total,
        'pages': (total + per_page - 1) // per_page,
        'has_prev': page > 1,
        'has_next': page * per_page < total,
        'prev_num': page - 1 if page > 1 else None,
        'next_num': page + 1 if page * per_page < total else None
    }
    
    return render_template('main/my_resources.html',
                         title='Mis Recursos',
                         folder_details=paginated_folders,
                         pagination=pagination_info,
                         current_filter=folder_filter)

@main_bp.route('/manage-resource/<int:folder_id>')
@login_required
def manage_resource(folder_id):
    """Manage permissions for a specific folder"""
    folder = Folder.query.get_or_404(folder_id)
    
    # Check if user can manage this folder
    if not current_user.can_validate_folder(folder):
        flash('No tienes permisos para gestionar esta carpeta.', 'error')
        return redirect(url_for('main.my_resources'))
    
    # Get current permissions
    read_permissions = folder.get_permissions_by_type('read')
    write_permissions = folder.get_permissions_by_type('write')
    
    # Get available AD groups for assignment
    from app.models import ADGroup
    available_groups = ADGroup.query.filter_by(is_active=True).order_by(ADGroup.name).all()
    
    # Get groups that already have permissions (to avoid duplicates)
    assigned_group_ids = set()
    for perm in folder.permissions:
        if perm.is_active:
            assigned_group_ids.add(perm.ad_group_id)
    
    available_groups = [g for g in available_groups if g.id not in assigned_group_ids]
    
    # Get comprehensive permissions summary (users and AD groups)
    permissions_summary = folder.get_permissions_summary()
    users_with_permissions = permissions_summary['users_with_permissions']
    
    # Get all active users for the assignment form
    all_active_users = User.query.filter_by(is_active=True).order_by(User.username).all()
    
    from datetime import datetime
    return render_template('main/manage_resource.html',
                         title=f'Gestionar: {folder.name}',
                         folder=folder,
                         read_permissions=read_permissions,
                         write_permissions=write_permissions,
                         available_groups=available_groups,
                         users_with_permissions=users_with_permissions,
                         permissions_summary=permissions_summary,
                         all_active_users=all_active_users,
                         current_date=datetime.now(),
                         is_owner=folder in current_user.owned_folders,
                         is_validator=folder in current_user.validated_folders)

@main_bp.route('/grant-permission/<int:folder_id>', methods=['POST'])
@login_required
def grant_permission(folder_id):
    """Grant permission to an AD group for a folder"""
    folder = Folder.query.get_or_404(folder_id)
    
    # Check if user can manage this folder
    if not current_user.can_validate_folder(folder):
        flash('No tienes permisos para gestionar esta carpeta.', 'error')
        return redirect(url_for('main.my_resources'))
    
    ad_group_id = request.form.get('ad_group_id', type=int)
    permission_type = request.form.get('permission_type')
    
    if not ad_group_id or not permission_type or permission_type not in ['read', 'write']:
        flash('Datos de solicitud inválidos.', 'error')
        return redirect(url_for('main.manage_resource', folder_id=folder_id))
    
    # Check if permission already exists
    existing_permission = FolderPermission.query.filter_by(
        folder_id=folder_id,
        ad_group_id=ad_group_id,
        permission_type=permission_type,
        is_active=True
    ).first()
    
    if existing_permission:
        flash('El grupo ya tiene este tipo de permiso para esta carpeta.', 'warning')
        return redirect(url_for('main.manage_resource', folder_id=folder_id))
    
    # Create new permission
    from app.models import ADGroup
    ad_group = ADGroup.query.get(ad_group_id)
    if not ad_group:
        flash('Grupo AD no encontrado.', 'error')
        return redirect(url_for('main.manage_resource', folder_id=folder_id))
    
    new_permission = FolderPermission(
        folder_id=folder_id,
        ad_group_id=ad_group_id,
        permission_type=permission_type,
        granted_by=current_user,
        is_active=True
    )
    
    db.session.add(new_permission)
    db.session.flush()  # Get the permission ID
    
    # Create tasks for applying changes (Airflow DAG + AD verification)
    task = None
    try:
        from app.services.task_service import create_permission_task
        
        # Create task for direct permission grant
        task = create_permission_task(
            action='grant',
            folder=folder,
            ad_group=ad_group,
            permission_type=permission_type,
            created_by=current_user
        )
        
        if task:
            current_app.logger.info(f"Created permission grant task {task.id} for folder {folder_id}")
        else:
            current_app.logger.warning(f"Failed to create permission grant task for folder {folder_id}")
        
    except Exception as e:
        current_app.logger.error(f"Error creating tasks for direct permission grant: {str(e)}")
        # Continue even if task creation fails
    
    db.session.commit()
    
    # Log audit event
    AuditEvent.log_event(
        user=current_user,
        event_type='permission_grant',
        action='grant',
        resource_type='folder_permission',
        resource_id=new_permission.id,
        description=f'Otorgado permiso {permission_type} al grupo {ad_group.name} para carpeta {folder.path}',
        metadata={
            'folder_path': folder.path,
            'ad_group_name': ad_group.name,
            'permission_type': permission_type,
            'task_created': task.id if task else None
        },
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    flash(f'Permiso de {permission_type} otorgado al grupo {ad_group.name}. Tarea creada para aplicar cambios.', 'success')
    return redirect(url_for('main.manage_resource', folder_id=folder_id))

@main_bp.route('/revoke-permission/<int:permission_id>', methods=['POST'])
@login_required
def revoke_permission(permission_id):
    """Revoke a specific permission"""
    permission = FolderPermission.query.get_or_404(permission_id)
    folder = permission.folder
    
    # Check if user can manage this folder
    if not current_user.can_validate_folder(folder):
        flash('No tienes permisos para gestionar esta carpeta.', 'error')
        return redirect(url_for('main.my_resources'))
    
    # Mark permission as inactive instead of deleting
    permission.is_active = False
    
    # Create task for applying changes
    from app.services.task_service import create_permission_task
    task = create_permission_task(
        action='revoke',
        folder=folder,
        ad_group=permission.ad_group,
        permission_type=permission.permission_type,
        created_by=current_user
    )
    
    db.session.commit()
    
    # Log audit event
    AuditEvent.log_event(
        user=current_user,
        event_type='permission_revoke',
        action='revoke',
        resource_type='folder_permission',
        resource_id=permission.id,
        description=f'Revocado permiso {permission.permission_type} del grupo {permission.ad_group.name} para carpeta {folder.path}',
        metadata={
            'folder_path': folder.path,
            'ad_group_name': permission.ad_group.name,
            'permission_type': permission.permission_type,
            'task_id': task.id if task else None
        },
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    flash(f'Permiso de {permission.permission_type} revocado del grupo {permission.ad_group.name}. Tarea creada para aplicar cambios.', 'success')
    return redirect(url_for('main.manage_resource', folder_id=folder.id))

@main_bp.route('/api/folder/<int:folder_id>/validators')
@login_required
def get_folder_validators(folder_id):
    """API endpoint to get validators for a specific folder"""
    folder = Folder.query.get_or_404(folder_id)
    
    # Get authorized validators for this folder (only owners and validators, no admins)
    authorized_validators = []
    
    # Propietarios de la carpeta
    for owner in folder.owners:
        authorized_validators.append({
            'id': owner.id,
            'name': owner.full_name,
            'role': 'Propietario',
            'display_name': f"{owner.full_name} (Propietario)"
        })
    
    # Validadores específicos de la carpeta
    for validator in folder.validators:
        if validator not in folder.owners:  # Evitar duplicados
            authorized_validators.append({
                'id': validator.id,
                'name': validator.full_name,
                'role': 'Validador',
                'display_name': f"{validator.full_name} (Validador)"
            })
    
    # Ordenar por nombre
    authorized_validators.sort(key=lambda x: x['name'])
    
    return jsonify({
        'success': True,
        'validators': authorized_validators,
        'folder': {
            'id': folder.id,
            'name': folder.name,
            'path': folder.path
        }
    })

@main_bp.route('/permission-details/<int:folder_id>/<int:user_id>')
@login_required
def permission_details(folder_id, user_id):
    """View detailed permission information for a specific user on a folder"""
    folder = Folder.query.get_or_404(folder_id)
    user = User.query.get_or_404(user_id)
    
    # Check if current user can manage this folder
    if not current_user.can_validate_folder(folder):
        flash('No tienes permisos para gestionar esta carpeta.', 'error')
        return redirect(url_for('main.my_resources'))
    
    # Get user's permissions for this folder
    users_with_permissions = folder.get_all_users_with_permissions()
    user_permissions = None
    
    for user_data in users_with_permissions:
        if user_data['user'].id == user_id:
            user_permissions = user_data
            break
    
    if not user_permissions:
        flash('El usuario no tiene permisos para esta carpeta.', 'error')
        return redirect(url_for('main.manage_resource', folder_id=folder_id))
    
    from datetime import datetime
    return render_template('main/permission_details.html',
                         title=f'Permisos de {user.full_name}',
                         folder=folder,
                         user=user,
                         user_permissions=user_permissions,
                         current_date=datetime.now())

@main_bp.route('/update-folder-validators/<int:folder_id>', methods=['POST'])
@login_required
def update_folder_validators(folder_id):
    """Update validators for a folder (only owners can do this)"""
    folder = Folder.query.get_or_404(folder_id)
    
    # Check if user is an owner of this folder
    if folder not in current_user.owned_folders:
        flash('Solo los propietarios pueden modificar los validadores de una carpeta.', 'error')
        return redirect(url_for('main.permission_details', folder_id=folder_id, user_id=request.referrer_user_id if 'referrer_user_id' in request.form else 1))
    
    # Get the selected validator IDs from the form
    validator_ids = request.form.getlist('validators')
    
    try:
        # Convert IDs to integers and get user objects
        validator_ids = [int(id) for id in validator_ids if id.strip()]
        selected_validators = User.query.filter(
            User.id.in_(validator_ids),
            User.is_active == True
        ).all() if validator_ids else []
        
        # Update validators
        folder.validators = selected_validators
        db.session.commit()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='folder_validators',
            action='update',
            resource_type='folder',
            resource_id=folder.id,
            description=f'Validadores actualizados para carpeta {folder.path}',
            metadata={
                'folder_path': folder.path,
                'validators': [v.username for v in selected_validators]
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        flash(f'Validadores actualizados exitosamente. Se han asignado {len(selected_validators)} validadores.', 'success')
        
    except ValueError as e:
        flash('Error en los datos enviados. Por favor, intenta de nuevo.', 'error')
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error updating folder validators: {str(e)}")
        flash('Error al actualizar validadores. Por favor, intenta de nuevo.', 'error')
    
    # Return to the permission details page - we need to get a user_id from somewhere
    # Let's check if there's a referrer or default to the folder management page
    return redirect(url_for('main.manage_resource', folder_id=folder_id))

@main_bp.route('/assign-user-permission/<int:folder_id>', methods=['POST'])
@login_required
def assign_user_permission(folder_id):
    """Assign direct permission to a user for a folder"""
    folder = Folder.query.get_or_404(folder_id)
    
    # Check if user can manage this folder
    if not current_user.can_validate_folder(folder):
        flash('No tienes permisos para gestionar esta carpeta.', 'error')
        return redirect(url_for('main.my_resources'))
    
    user_id = request.form.get('user_id', type=int)
    permission_type = request.form.get('permission_type')
    notes = request.form.get('notes', '').strip()
    
    if not user_id or not permission_type or permission_type not in ['read', 'write']:
        flash('Datos de solicitud inválidos.', 'error')
        return redirect(url_for('main.manage_resource', folder_id=folder_id))
    
    user = User.query.get(user_id)
    if not user or not user.is_active:
        flash('Usuario no encontrado o inactivo.', 'error')
        return redirect(url_for('main.manage_resource', folder_id=folder_id))
    
    # Check if permission already exists
    existing_permission = UserFolderPermission.query.filter_by(
        user_id=user_id,
        folder_id=folder_id,
        permission_type=permission_type,
        is_active=True
    ).first()
    
    if existing_permission:
        flash(f'El usuario ya tiene permiso de {permission_type} para esta carpeta.', 'warning')
        return redirect(url_for('main.manage_resource', folder_id=folder_id))
    
    # Create new user permission (no expiration date)
    expires_at = None  # Define the variable for use in metadata
    new_permission = UserFolderPermission(
        user_id=user_id,
        folder_id=folder_id,
        permission_type=permission_type,
        granted_by_id=current_user.id,
        expires_at=expires_at,
        notes=notes,
        is_active=True
    )
    
    db.session.add(new_permission)
    
    # Create task for applying changes
    from app.services.task_service import create_user_permission_task
    task = create_user_permission_task(
        action='grant',
        folder=folder,
        user=user,
        permission_type=permission_type,
        created_by=current_user,
        notes=notes,
        expires_at=None
    )
    
    db.session.commit()
    
    # Log audit event
    AuditEvent.log_event(
        user=current_user,
        event_type='user_permission_grant',
        action='grant',
        resource_type='user_folder_permission',
        resource_id=new_permission.id,
        description=f'Otorgado permiso {permission_type} al usuario {user.username} para carpeta {folder.path}',
        metadata={
            'folder_path': folder.path,
            'target_username': user.username,
            'permission_type': permission_type,
            'expires_at': expires_at,
            'notes': notes,
            'task_id': task.id if task else None
        },
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    flash(f'Permiso de {permission_type} otorgado al usuario {user.username}. Tarea creada para aplicar cambios.', 'success')
    return redirect(url_for('main.manage_resource', folder_id=folder_id))

@main_bp.route('/revoke-user-permission/<int:permission_id>', methods=['POST'])
@login_required
def revoke_user_permission(permission_id):
    """Revoke a specific user permission"""
    permission = UserFolderPermission.query.get_or_404(permission_id)
    folder = permission.folder
    user = permission.user
    
    # Check if user can manage this folder
    if not current_user.can_validate_folder(folder):
        flash('No tienes permisos para gestionar esta carpeta.', 'error')
        return redirect(url_for('main.my_resources'))
    
    # Mark permission as inactive instead of deleting
    permission.is_active = False
    
    # Create task for applying changes
    from app.services.task_service import create_user_permission_task
    task = create_user_permission_task(
        action='revoke',
        folder=folder,
        user=user,
        permission_type=permission.permission_type,
        created_by=current_user
    )
    
    db.session.commit()
    
    # Log audit event
    AuditEvent.log_event(
        user=current_user,
        event_type='user_permission_revoke',
        action='revoke',
        resource_type='user_folder_permission',
        resource_id=permission.id,
        description=f'Revocado permiso {permission.permission_type} del usuario {user.username} para carpeta {folder.path}',
        metadata={
            'folder_path': folder.path,
            'target_username': user.username,
            'permission_type': permission.permission_type,
            'task_id': task.id if task else None
        },
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    flash(f'Permiso de {permission.permission_type} revocado del usuario {user.username}. Tarea creada para aplicar cambios.', 'success')
    return redirect(url_for('main.manage_resource', folder_id=folder.id))

