from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify, current_app, send_file
from flask_login import login_required, current_user
from app.models import User, Role, Folder, ADGroup, FolderPermission, PermissionRequest, AuditEvent, Task, UserADGroupMembership
from app.forms import UserForm, FolderForm, ADGroupForm
from app.services.ldap_service import LDAPService
from app import db
from functools import wraps
from datetime import datetime
import os
import json
import zipfile
import tempfile
import shutil
from sqlalchemy import text

# Configuration constants
BACKUP_DIRECTORY = '/app/backups'

admin_bp = Blueprint('admin', __name__)

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin():
            flash('Acceso denegado. Se requieren permisos de administrador.', 'error')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated_function

@admin_bp.route('/')
@login_required
@admin_required
def admin_dashboard():
    stats = {
        'total_users': User.query.filter_by(is_active=True).count(),
        'total_folders': Folder.query.filter_by(is_active=True).count(),
        'total_ad_groups': ADGroup.query.filter_by(is_active=True).count(),
        'pending_requests': PermissionRequest.query.filter_by(status='pending').count(),
        'total_permissions': FolderPermission.query.filter_by(is_active=True).count(),
        'active_permissions': FolderPermission.query.filter_by(is_active=True).count(),
        'total_tasks': Task.query.count(),
        'pending_tasks': Task.query.filter_by(status='pending').count(),
        'running_tasks': Task.query.filter_by(status='running').count(),
        'failed_tasks': Task.query.filter_by(status='failed').count(),
        'completed_tasks': Task.query.filter_by(status='completed').count()
    }
    
    # Recent audit events
    recent_events = AuditEvent.query.order_by(AuditEvent.created_at.desc()).limit(10).all()
    
    # Recent permission requests
    recent_requests = PermissionRequest.query.order_by(
        PermissionRequest.created_at.desc()
    ).limit(10).all()
    
    return render_template('admin/dashboard.html',
                         title='Panel de Administración',
                         stats=stats,
                         recent_events=recent_events,
                         recent_requests=recent_requests)

# User Management
@admin_bp.route('/users')
@login_required
@admin_required
def users():
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '')
    
    query = User.query
    if search:
        query = query.filter(
            db.or_(
                User.username.ilike(f'%{search}%'),
                User.full_name.ilike(f'%{search}%'),
                User.email.ilike(f'%{search}%')
            )
        )
    
    users = query.order_by(User.full_name).paginate(
        page=page, per_page=20, error_out=False
    )
    
    return render_template('admin/users.html', 
                         title='Gestión de Usuarios',
                         users=users, search=search)


@admin_bp.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_user(user_id):
    user = User.query.get_or_404(user_id)
    form = UserForm(obj=user)
    
    if form.validate_on_submit():
        user.username = form.username.data.lower()
        user.email = form.email.data
        user.full_name = form.full_name.data
        user.department = form.department.data
        user.is_active = form.is_active.data
        
        # Update roles
        selected_roles = Role.query.filter(Role.id.in_(form.roles.data)).all()
        user.roles = selected_roles
        
        db.session.commit()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='user_management',
            action='update',
            resource_type='user',
            resource_id=user.id,
            description=f'Usuario {user.username} actualizado',
            metadata={'username': user.username, 'roles': [r.name for r in selected_roles]},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        flash(f'Usuario {user.username} actualizado exitosamente.', 'success')
        return redirect(url_for('admin.users'))
    
    # Pre-populate roles
    form.roles.data = [role.id for role in user.roles]
    
    return render_template('admin/user_form.html', 
                         title='Editar Usuario', form=form, user=user)

@admin_bp.route('/users/<int:user_id>/toggle', methods=['POST'])
@login_required
@admin_required
def toggle_user_status(user_id):
    """Toggle user active status"""
    try:
        user = User.query.get_or_404(user_id)
        
        # Prevent deactivating yourself
        if user.id == current_user.id:
            return jsonify({
                'success': False,
                'message': 'No puedes desactivar tu propio usuario'
            }), 400
        
        # Toggle status
        old_status = user.is_active
        user.is_active = not user.is_active
        
        db.session.commit()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='user_management',
            action='toggle_status',
            resource_type='user',
            resource_id=user.id,
            description=f'Usuario {user.username} {"activado" if user.is_active else "desactivado"}',
            metadata={
                'username': user.username,
                'old_status': old_status,
                'new_status': user.is_active
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': f'Usuario {user.username} {"activado" if user.is_active else "desactivado"} exitosamente',
            'is_active': user.is_active
        })
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'Error toggling user {user_id} status: {str(e)}')
        return jsonify({
            'success': False,
            'message': f'Error al cambiar estado del usuario: {str(e)}'
        }), 500

@admin_bp.route('/users/sync')
@login_required
@admin_required
def sync_users():
    """Sync users from LDAP"""
    try:
        ldap_service = LDAPService()
        synced_count = ldap_service.sync_users()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='user_sync',
            action='sync_users',
            description=f'Sincronización de usuarios AD completada: {synced_count} usuarios',
            metadata={'synced_count': synced_count},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        flash(f'Sincronización completada. {synced_count} usuarios procesados.', 'success')
    except Exception as e:
        flash(f'Error durante la sincronización: {str(e)}', 'error')
    
    return redirect(url_for('admin.users'))

# Folder Management
@admin_bp.route('/folders')
@login_required
@admin_required
def folders():
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '')
    
    query = Folder.query
    if search:
        query = query.filter(
            db.or_(
                Folder.name.ilike(f'%{search}%'),
                Folder.path.ilike(f'%{search}%')
            )
        )
    
    folders = query.order_by(Folder.path).paginate(
        page=page, per_page=20, error_out=False
    )
    
    return render_template('admin/folders.html', 
                         title='Gestión de Carpetas',
                         folders=folders, search=search)

@admin_bp.route('/folders/new', methods=['GET', 'POST'])
@login_required
@admin_required
def new_folder():
    form = FolderForm()
    
    if form.validate_on_submit():
        folder = Folder(
            name=form.name.data,
            path=form.path.data,
            description=form.description.data,
            is_active=form.is_active.data
        )
        
        # Add owners and validators
        selected_owners = User.query.filter(User.id.in_(form.owners.data)).all()
        selected_validators = User.query.filter(User.id.in_(form.validators.data)).all()
        
        folder.owners = selected_owners
        folder.validators = selected_validators
        
        db.session.add(folder)
        db.session.flush()  # Flush to get folder ID
        
        # Add permissions (new folder, so no existing permissions to worry about)
        try:
            # Deduplicate group IDs and convert to integers to avoid constraint violations
            read_groups_raw = form.read_groups.data if form.read_groups.data else []
            write_groups_raw = form.write_groups.data if form.write_groups.data else []
            
            # Convert to integers and deduplicate
            unique_read_groups = list(set(int(g) for g in read_groups_raw if g and str(g).strip()))
            unique_write_groups = list(set(int(g) for g in write_groups_raw if g and str(g).strip()))
            
            # Create a comprehensive mapping of group -> permission type
            # Write permissions override read permissions
            group_permissions = {}
            
            # First, add all read permissions
            for group_id in unique_read_groups:
                group_permissions[group_id] = 'read'
            
            # Then override with write permissions (write includes read)
            for group_id in unique_write_groups:
                group_permissions[group_id] = 'write'
            
            # Now create permissions based on the final mapping
            for group_id, permission_type in group_permissions.items():
                # Double-check that permission doesn't already exist (should not happen in new folder)
                existing = FolderPermission.query.filter_by(
                    folder_id=folder.id,
                    ad_group_id=group_id,
                    permission_type=permission_type,
                    is_active=True
                ).first()
                
                if not existing:
                    permission = FolderPermission(
                        folder_id=folder.id,
                        ad_group_id=group_id,
                        permission_type=permission_type,
                        granted_by_id=current_user.id
                    )
                    db.session.add(permission)
                    
        except Exception as e:
            db.session.rollback()
            flash(f'Error al crear permisos: {str(e)}', 'error')
            return redirect(url_for('admin.new_folder'))
        
        db.session.commit()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='folder_management',
            action='create',
            resource_type='folder',
            resource_id=folder.id,
            description=f'Carpeta {folder.path} creada',
            metadata={
                'folder_path': folder.path,
                'owners': [o.username for o in selected_owners],
                'validators': [v.username for v in selected_validators]
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        flash(f'Carpeta {folder.path} creada exitosamente.', 'success')
        return redirect(url_for('admin.folders'))
    
    return render_template('admin/folder_form_improved.html', 
                         title='Nueva Carpeta', form=form)

@admin_bp.route('/folders/<int:folder_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_folder(folder_id):
    folder = Folder.query.get_or_404(folder_id)
    form = FolderForm(obj=folder)
    
    if form.validate_on_submit():
        folder.name = form.name.data
        folder.path = form.path.data
        folder.description = form.description.data
        folder.is_active = form.is_active.data
        
        # Update owners and validators
        selected_owners = User.query.filter(User.id.in_(form.owners.data)).all()
        selected_validators = User.query.filter(User.id.in_(form.validators.data)).all()
        
        folder.owners = selected_owners
        folder.validators = selected_validators
        
        # Update permissions - completely rewrite permission management logic
        try:
            # Get current permissions for comparison
            current_permissions = FolderPermission.query.filter_by(folder_id=folder.id).all()
            
            # Create sets for comparison
            current_read_groups = set(p.ad_group_id for p in current_permissions if p.permission_type == 'read')
            current_write_groups = set(p.ad_group_id for p in current_permissions if p.permission_type == 'write')
            
            new_read_groups = set(form.read_groups.data)
            new_write_groups = set(form.write_groups.data)
            
            # Remove permissions that are no longer needed
            for perm in current_permissions:
                if perm.permission_type == 'read' and perm.ad_group_id not in new_read_groups:
                    db.session.delete(perm)
                elif perm.permission_type == 'write' and perm.ad_group_id not in new_write_groups:
                    db.session.delete(perm)
            
            # Commit deletions before adding new permissions
            db.session.commit()
            
            # Add new read permissions
            for group_id in new_read_groups - current_read_groups:
                read_permission = FolderPermission(
                    folder_id=folder.id,
                    ad_group_id=group_id,
                    permission_type='read',
                    granted_by_id=current_user.id
                )
                db.session.add(read_permission)
            
            # Add new write permissions
            for group_id in new_write_groups - current_write_groups:
                write_permission = FolderPermission(
                    folder_id=folder.id,
                    ad_group_id=group_id,
                    permission_type='write',
                    granted_by_id=current_user.id
                )
                db.session.add(write_permission)
                    
        except Exception as e:
            db.session.rollback()
            flash(f'Error al actualizar permisos: {str(e)}', 'error')
            return redirect(url_for('admin.edit_folder', folder_id=folder.id))
        
        db.session.commit()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='folder_management',
            action='update',
            resource_type='folder',
            resource_id=folder.id,
            description=f'Carpeta {folder.path} actualizada',
            metadata={
                'folder_path': folder.path,
                'owners': [o.username for o in selected_owners],
                'validators': [v.username for v in selected_validators]
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        flash(f'Carpeta {folder.path} actualizada exitosamente.', 'success')
        return redirect(url_for('admin.folders'))
    
    # Pre-populate owners and validators
    form.owners.data = [owner.id for owner in folder.owners]
    form.validators.data = [validator.id for validator in folder.validators]
    
    # Pre-populate permission groups
    read_groups = [fp.ad_group_id for fp in folder.permissions if fp.permission_type == 'read']
    write_groups = [fp.ad_group_id for fp in folder.permissions if fp.permission_type == 'write']
    form.read_groups.data = read_groups
    form.write_groups.data = write_groups
    
    return render_template('admin/folder_form_improved.html', 
                         title='Editar Carpeta', form=form, folder=folder)

@admin_bp.route('/folders/export')
@login_required
@admin_required
def export_folders():
    """Export folders to CSV file"""
    import csv
    import io
    from flask import Response
    
    # Create CSV content
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow([
        'nombre', 'descripcion', 'ruta', 'propietario_username', 
        'validadores_usernames', 'grupo_lectura', 'grupo_escritura'
    ])
    
    # Get all active folders with their relationships
    folders = Folder.query.filter_by(is_active=True).all()
    
    for folder in folders:
        # Get owner username
        owner_username = folder.owners[0].username if folder.owners else ''
        
        # Get validators usernames
        validator_usernames = ';'.join([v.username for v in folder.validators])
        
        # Get read and write groups
        read_groups = []
        write_groups = []
        
        for permission in folder.permissions:
            if permission.is_active:
                if permission.permission_type == 'read':
                    read_groups.append(permission.ad_group.name)
                elif permission.permission_type == 'write':
                    write_groups.append(permission.ad_group.name)
        
        read_group_names = ';'.join(read_groups)
        write_group_names = ';'.join(write_groups)
        
        writer.writerow([
            folder.name,
            folder.description or '',
            folder.path,
            owner_username,
            validator_usernames,
            read_group_names,
            write_group_names
        ])
    
    # Create response
    output.seek(0)
    response = Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={"Content-Disposition": f"attachment; filename=folders_export.csv"}
    )
    
    # Log audit event
    AuditEvent.log_event(
        user=current_user,
        event_type='system_admin',
        action='export_folders',
        resource_type='folder',
        description='Exportación masiva de carpetas a CSV',
        ip_address=request.remote_addr,
        user_agent=request.headers.get('User-Agent')
    )
    
    return response

@admin_bp.route('/folders/import', methods=['POST'])
@login_required
@admin_required
def import_folders():
    """Import folders from CSV file"""
    import csv
    import io
    
    if 'csvFile' not in request.files:
        flash('No se seleccionó ningún archivo', 'error')
        return redirect(url_for('admin.folders'))
    
    file = request.files['csvFile']
    if file.filename == '':
        flash('No se seleccionó ningún archivo', 'error')
        return redirect(url_for('admin.folders'))
    
    if not file.filename.lower().endswith('.csv'):
        flash('El archivo debe ser un CSV', 'error')
        return redirect(url_for('admin.folders'))
    
    try:
        # Read CSV content
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_input = csv.DictReader(stream)
        
        # Track statistics
        created_count = 0
        updated_count = 0
        error_count = 0
        errors = []
        
        for row_num, row in enumerate(csv_input, start=2):
            try:
                # Validate required fields
                if not all([row.get('nombre'), row.get('ruta')]):
                    errors.append(f'Fila {row_num}: Faltan campos obligatorios (nombre, ruta)')
                    error_count += 1
                    continue
                
                # Check if folder exists
                existing_folder = Folder.query.filter_by(path=row['ruta']).first()
                
                if existing_folder:
                    # Update existing folder
                    existing_folder.name = row['nombre']
                    existing_folder.description = row.get('descripcion', '')
                    folder = existing_folder
                    updated_count += 1
                else:
                    # Create new folder
                    folder = Folder(
                        name=row['nombre'],
                        description=row.get('descripcion', ''),
                        path=row['ruta'],
                        created_by_id=current_user.id
                    )
                    db.session.add(folder)
                    created_count += 1
                
                db.session.flush()  # Get folder ID
                
                # Handle owner
                if row.get('propietario_username'):
                    owner = User.query.filter_by(username=row['propietario_username']).first()
                    if owner:
                        # Clear existing owners and add new one
                        folder.owners.clear()
                        folder.owners.append(owner)
                    else:
                        errors.append(f'Fila {row_num}: Usuario propietario "{row["propietario_username"]}" no encontrado')
                
                # Handle validators
                if row.get('validadores_usernames'):
                    folder.validators.clear()
                    validator_usernames = [u.strip() for u in row['validadores_usernames'].split(';') if u.strip()]
                    for username in validator_usernames:
                        validator = User.query.filter_by(username=username).first()
                        if validator:
                            folder.validators.append(validator)
                        else:
                            errors.append(f'Fila {row_num}: Usuario validador "{username}" no encontrado')
                
                # Handle read groups
                if row.get('grupo_lectura'):
                    # Remove existing read permissions
                    FolderPermission.query.filter_by(
                        folder_id=folder.id, 
                        permission_type='read'
                    ).delete()
                    
                    group_names = [g.strip() for g in row['grupo_lectura'].split(';') if g.strip()]
                    for group_name in group_names:
                        ad_group = ADGroup.query.filter_by(name=group_name).first()
                        if ad_group:
                            permission = FolderPermission(
                                folder_id=folder.id,
                                ad_group_id=ad_group.id,
                                permission_type='read',
                                granted_by_id=current_user.id
                            )
                            db.session.add(permission)
                        else:
                            errors.append(f'Fila {row_num}: Grupo AD de lectura "{group_name}" no encontrado')
                
                # Handle write groups
                if row.get('grupo_escritura'):
                    # Remove existing write permissions
                    FolderPermission.query.filter_by(
                        folder_id=folder.id, 
                        permission_type='write'
                    ).delete()
                    
                    group_names = [g.strip() for g in row['grupo_escritura'].split(';') if g.strip()]
                    for group_name in group_names:
                        ad_group = ADGroup.query.filter_by(name=group_name).first()
                        if ad_group:
                            permission = FolderPermission(
                                folder_id=folder.id,
                                ad_group_id=ad_group.id,
                                permission_type='write',
                                granted_by_id=current_user.id
                            )
                            db.session.add(permission)
                        else:
                            errors.append(f'Fila {row_num}: Grupo AD de escritura "{group_name}" no encontrado')
                
            except Exception as e:
                errors.append(f'Fila {row_num}: Error procesando - {str(e)}')
                error_count += 1
                continue
        
        # Commit all changes
        db.session.commit()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='system_admin',
            action='import_folders',
            resource_type='folder',
            description=f'Importación masiva de carpetas: {created_count} creadas, {updated_count} actualizadas, {error_count} errores',
            metadata={
                'created_count': created_count,
                'updated_count': updated_count,
                'error_count': error_count,
                'errors': errors[:10]  # Only store first 10 errors
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        # Show results
        success_msg = f'Importación completada: {created_count} carpetas creadas, {updated_count} actualizadas'
        if error_count > 0:
            success_msg += f', {error_count} errores'
        
        flash(success_msg, 'success' if error_count == 0 else 'warning')
        
        if errors and error_count <= 10:
            for error in errors:
                flash(error, 'error')
        elif error_count > 10:
            flash(f'Se encontraron {error_count} errores adicionales. Revise el log de auditoría para más detalles.', 'warning')
            
    except Exception as e:
        db.session.rollback()
        flash(f'Error procesando el archivo CSV: {str(e)}', 'error')
    
    return redirect(url_for('admin.folders'))

@admin_bp.route('/folders/<int:folder_id>/toggle', methods=['POST'])
@login_required
@admin_required
def toggle_folder_status(folder_id):
    """Toggle folder active status"""
    try:
        folder = Folder.query.get_or_404(folder_id)
        
        # Toggle status
        folder.is_active = not folder.is_active
        
        # Log audit event
        action = 'activate_folder' if folder.is_active else 'deactivate_folder'
        status_text = 'activada' if folder.is_active else 'desactivada'
        
        AuditEvent.log_event(
            user=current_user,
            event_type='folder_management',
            action=action,
            resource_type='folder',
            resource_id=folder.id,
            description=f'Carpeta "{folder.name}" {status_text}',
            metadata={
                'folder_id': folder.id,
                'folder_name': folder.name,
                'folder_path': folder.path,
                'new_status': folder.is_active
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': f'Carpeta {status_text} exitosamente',
            'new_status': folder.is_active
        })
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'Error toggling folder status: {str(e)}')
        return jsonify({
            'success': False,
            'message': f'Error al cambiar el estado: {str(e)}'
        }), 500

@admin_bp.route('/folders/<int:folder_id>/delete', methods=['POST'])
@login_required
@admin_required
def delete_folder(folder_id):
    """Delete folder and all related data"""
    try:
        folder = Folder.query.get_or_404(folder_id)
        
        # Store folder info for audit log before deletion
        folder_info = {
            'id': folder.id,
            'name': folder.name,
            'path': folder.path,
            'description': folder.description,
            'is_active': folder.is_active
        }
        
        # Get counts before deletion for audit log
        permissions_count = FolderPermission.query.filter_by(folder_id=folder.id).count()
        requests_count = PermissionRequest.query.filter_by(folder_id=folder.id).count()
        tasks_count = Task.query.join(PermissionRequest).filter(PermissionRequest.folder_id == folder.id).count()
        
        # Delete related data in proper order to avoid foreign key constraints
        
        # 1. Delete tasks related to permission requests for this folder
        tasks_to_delete = Task.query.join(PermissionRequest).filter(PermissionRequest.folder_id == folder.id).all()
        for task in tasks_to_delete:
            db.session.delete(task)
        
        # 2. Delete permission requests for this folder
        requests_to_delete = PermissionRequest.query.filter_by(folder_id=folder.id).all()
        for permission_request in requests_to_delete:
            db.session.delete(permission_request)
        
        # 3. Delete folder permissions
        permissions_to_delete = FolderPermission.query.filter_by(folder_id=folder.id).all()
        for permission in permissions_to_delete:
            db.session.delete(permission)
        
        # 4. Clear relationships (many-to-many)
        folder.owners.clear()
        folder.validators.clear()
        
        # 5. Finally delete the folder
        db.session.delete(folder)
        
        # Log comprehensive audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='folder_management',
            action='delete_folder',
            resource_type='folder',
            resource_id=folder_info['id'],
            description=f'Carpeta "{folder_info["name"]}" eliminada permanentemente',
            metadata={
                'deleted_folder': folder_info,
                'deleted_permissions_count': permissions_count,
                'deleted_requests_count': requests_count,
                'deleted_tasks_count': tasks_count,
                'deletion_timestamp': datetime.utcnow().isoformat()
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        db.session.commit()
        
        current_app.logger.info(f'Folder {folder_info["name"]} (ID: {folder_info["id"]}) deleted by {current_user.username}')
        
        return jsonify({
            'success': True,
            'message': f'Carpeta "{folder_info["name"]}" eliminada exitosamente',
            'deleted_data': {
                'permissions': permissions_count,
                'requests': requests_count,
                'tasks': tasks_count
            }
        })
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f'Error deleting folder {folder_id}: {str(e)}')
        import traceback
        current_app.logger.error(f'Full traceback: {traceback.format_exc()}')
        return jsonify({
            'success': False,
            'message': f'Error al eliminar la carpeta: {str(e)}'
        }), 500

# AD Groups Management
@admin_bp.route('/ad-groups')
@login_required
@admin_required
def ad_groups():
    page = request.args.get('page', 1, type=int)
    search = request.args.get('search', '')
    
    query = ADGroup.query
    if search:
        query = query.filter(
            db.or_(
                ADGroup.name.ilike(f'%{search}%'),
                ADGroup.description.ilike(f'%{search}%')
            )
        )
    
    groups = query.order_by(ADGroup.name).paginate(
        page=page, per_page=20, error_out=False
    )
    
    return render_template('admin/ad_groups.html', 
                         title='Gestión de Grupos AD',
                         groups=groups, search=search)

@admin_bp.route('/ad-groups/sync')
@login_required
@admin_required
def sync_ad_groups():
    """Sync AD groups from LDAP"""
    try:
        ldap_service = LDAPService()
        synced_count = ldap_service.sync_groups()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='ad_sync',
            action='sync_groups',
            description=f'Sincronización de grupos AD completada: {synced_count} grupos',
            metadata={'synced_count': synced_count},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        flash(f'Sincronización completada. {synced_count} grupos procesados.', 'success')
    except Exception as e:
        flash(f'Error durante la sincronización: {str(e)}', 'error')
    
    return redirect(url_for('admin.ad_groups'))

# Audit and Reports
@admin_bp.route('/audit')
@login_required
@admin_required
def audit():
    page = request.args.get('page', 1, type=int)
    event_type = request.args.get('event_type', '')
    user_search = request.args.get('user_search', '').strip()
    
    query = AuditEvent.query
    
    if event_type:
        query = query.filter_by(event_type=event_type)
    
    # Filter by username if user search is provided
    if user_search:
        # Join with User table and filter by username or full_name
        query = query.join(User, AuditEvent.user_id == User.id).filter(
            db.or_(
                User.username.ilike(f'%{user_search}%'),
                User.full_name.ilike(f'%{user_search}%')
            )
        )
    
    events = query.order_by(AuditEvent.created_at.desc()).paginate(
        page=page, per_page=50, error_out=False
    )
    
    # Get unique event types for filter
    event_types = db.session.query(AuditEvent.event_type).distinct().all()
    event_types = [et[0] for et in event_types]
    
    return render_template('admin/audit.html', 
                         title='Auditoría del Sistema',
                         events=events,
                         event_types=event_types,
                         selected_event_type=event_type,
                         user_search=user_search)

@admin_bp.route('/reports/permissions')
@login_required
@admin_required
def permissions_report():
    """Generate comprehensive permissions report with filters"""
    from datetime import datetime
    
    # Get filter parameters
    folder_id = request.args.get('folder_id', '').strip()
    group_id = request.args.get('group_id', '').strip()
    folder_search = request.args.get('folder_search', '').strip()
    group_search = request.args.get('group_search', '').strip()
    permission_type = request.args.get('permission_type', 'all')
    status = request.args.get('status', 'active')
    
    # Build query with filters
    query = FolderPermission.query.join(Folder).join(ADGroup)
    
    # Filter by specific folder
    if folder_id and folder_id.isdigit():
        query = query.filter(Folder.id == int(folder_id))
    
    # Filter by specific group
    if group_id and group_id.isdigit():
        query = query.filter(ADGroup.id == int(group_id))
    
    # Filter by permission type
    if permission_type and permission_type != 'all':
        query = query.filter(FolderPermission.permission_type == permission_type)
    
    # Filter by status
    if status == 'active':
        query = query.filter(FolderPermission.is_active == True)
    elif status == 'inactive':
        query = query.filter(FolderPermission.is_active == False)
    # 'all' shows both active and inactive
    
    permissions = query.order_by(Folder.path, ADGroup.name).all()
    
    # Current filter values
    filters = {
        'permission_type': permission_type,
        'status': status
    }
    
    return render_template('admin/permissions_report.html',
                         title='Informe carpetas',
                         permissions=permissions,
                         filters=filters,
                         folder_id=folder_id,
                         group_id=group_id,
                         folder_search=folder_search,
                         group_search=group_search,
                         now=datetime.now())

@admin_bp.route('/reports/active-permissions')
@login_required
@admin_required
def active_permissions_report():
    """Generate active permissions report showing all users' effective permissions through AD groups"""
    from datetime import datetime
    from app.models import PermissionRequest, UserADGroupMembership, FolderPermission, Folder
    
    try:
        # Get filter parameters
        folder_id = request.args.get('folder_id', '').strip()
        user_search = request.args.get('user_search', '').strip()
        permission_type = request.args.get('permission_type', 'all')
        
        # Pagination parameters
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)  # Default 20 users per page
        
        permissions_by_user = {}
        all_permissions = []
        
        # STEP 1: Get approved permission requests
        query = PermissionRequest.query.filter_by(status='approved')
        
        # Apply filters
        if folder_id and folder_id.isdigit():
            query = query.filter(PermissionRequest.folder_id == int(folder_id))
        
        if permission_type and permission_type != 'all':
            query = query.filter(PermissionRequest.permission_type == permission_type)
        
        approved_requests = query.options(
            db.joinedload(PermissionRequest.requester),
            db.joinedload(PermissionRequest.folder),
            db.joinedload(PermissionRequest.ad_group)
        ).all()
        
        for permission in approved_requests:
            if not permission.folder or not permission.folder.is_active:
                continue
                
            # Apply user search filter
            if user_search:
                if not (user_search.lower() in permission.requester.username.lower() or 
                       user_search.lower() in (permission.requester.full_name or '').lower()):
                    continue
            
            user_id = permission.requester.id
            folder_id_key = permission.folder_id
            
            if user_id not in permissions_by_user:
                permissions_by_user[user_id] = {
                    'user': permission.requester,
                    'folders': {}
                }
            
            if folder_id_key not in permissions_by_user[user_id]['folders']:
                permissions_by_user[user_id]['folders'][folder_id_key] = {
                    'folder': permission.folder,
                    'permissions': []
                }
            
            permissions_by_user[user_id]['folders'][folder_id_key]['permissions'].append(permission)
            all_permissions.append(permission)
        
        # STEP 2: Get AD memberships
        memberships = UserADGroupMembership.query.filter_by(is_active=True).all()
        
        for membership in memberships:
            if not membership.user or not membership.ad_group:
                continue
            
            # Apply user search filter
            if user_search:
                if not (user_search.lower() in membership.user.username.lower() or 
                       user_search.lower() in (membership.user.full_name or '').lower()):
                    continue
            
            # Get folder permissions for this AD group
            folder_perms = FolderPermission.query.filter_by(
                ad_group_id=membership.ad_group.id,
                is_active=True
            ).all()
            
            for fp in folder_perms:
                if not fp.folder or not fp.folder.is_active:
                    continue
                
                # Apply filters
                if folder_id and folder_id.isdigit() and fp.folder.id != int(folder_id):
                    continue
                
                if permission_type and permission_type != 'all' and fp.permission_type != permission_type:
                    continue
                
                user_id = membership.user.id
                folder_id_key = fp.folder.id
                
                # Initialize user entry if not exists
                if user_id not in permissions_by_user:
                    permissions_by_user[user_id] = {
                        'user': membership.user,
                        'folders': {}
                    }
                
                # Initialize folder entry if not exists
                if folder_id_key not in permissions_by_user[user_id]['folders']:
                    permissions_by_user[user_id]['folders'][folder_id_key] = {
                        'folder': fp.folder,
                        'permissions': []
                    }
                
                # Check if this permission already exists (avoid duplicates)
                exists = False
                for existing in permissions_by_user[user_id]['folders'][folder_id_key]['permissions']:
                    if (hasattr(existing, 'permission_type') and 
                        existing.permission_type == fp.permission_type):
                        exists = True
                        break
                
                if not exists:
                    # Create virtual permission
                    class VirtualPermission:
                        def __init__(self, membership, folder_permission):
                            self.id = f"sync_{membership.id}_{folder_permission.id}"
                            self.folder_id = folder_permission.folder.id
                            self.folder = folder_permission.folder
                            self.permission_type = folder_permission.permission_type
                            self.ad_group = membership.ad_group
                            self.validator = None
                            self.validated_at = None
                            self.requester_id = membership.user.id
                            self.requester = membership.user
                            self.source = 'ad_sync'
                    
                    virtual_perm = VirtualPermission(membership, fp)
                    permissions_by_user[user_id]['folders'][folder_id_key]['permissions'].append(virtual_perm)
                    all_permissions.append(virtual_perm)
        
        # Get all folders for filter dropdown
        all_folders = Folder.query.filter_by(is_active=True).order_by(Folder.name).all()
        
        # Implement pagination for user records
        total_records = len(permissions_by_user)
        user_items = list(permissions_by_user.items())
        
        # Sort user records by username for consistent pagination
        user_items.sort(key=lambda x: x[1]['user'].username.lower())
        
        # Calculate pagination
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_user_items = user_items[start_idx:end_idx]
        
        # Convert back to dict for template
        paginated_permissions_by_user = dict(paginated_user_items)
        
        # Calculate pagination info
        total_pages = (total_records + per_page - 1) // per_page
        has_prev = page > 1
        has_next = page < total_pages
        prev_num = page - 1 if has_prev else None
        next_num = page + 1 if has_next else None
        
        pagination = {
            'page': page,
            'per_page': per_page,
            'total': total_records,
            'pages': total_pages,
            'has_prev': has_prev,
            'has_next': has_next,
            'prev_num': prev_num,
            'next_num': next_num
        }
        
        return render_template('admin/active_permissions_report.html',
                             title='Permisos activos',
                             permissions_by_user=paginated_permissions_by_user,
                             approved_permissions=all_permissions,
                             all_folders=all_folders,
                             filters={'permission_type': permission_type},
                             folder_id=folder_id,
                             user_search=user_search,
                             pagination=pagination,
                             now=datetime.now())
                             
    except Exception as e:
        current_app.logger.error(f"Error in active_permissions_report: {str(e)}")
        import traceback
        current_app.logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Return error message to user
        flash(f'Error en el reporte de permisos: {str(e)}', 'error')
        return redirect(url_for('admin.admin_dashboard'))


# Task Management
@admin_bp.route('/tasks')
@login_required
@admin_required
def task_monitor():
    """Task monitoring page for admins"""
    # Get task summary statistics
    from app.models import Task
    
    total_tasks = Task.query.count()
    pending_tasks = Task.query.filter_by(status='pending').count()
    running_tasks = Task.query.filter_by(status='running').count()
    completed_tasks = Task.query.filter_by(status='completed').count()
    failed_tasks = Task.query.filter_by(status='failed').count()
    retry_tasks = Task.query.filter_by(status='retry').count()
    
    # Get recent tasks
    recent_tasks = Task.query.order_by(Task.created_at.desc()).limit(10).all()
    
    stats = {
        'total_tasks': total_tasks,
        'pending_tasks': pending_tasks,
        'running_tasks': running_tasks,
        'completed_tasks': completed_tasks,
        'failed_tasks': failed_tasks,
        'retry_tasks': retry_tasks
    }
    
    return render_template('main/task_monitor.html', 
                         title='Monitor de Tareas',
                         stats=stats,
                         recent_tasks=recent_tasks)

@admin_bp.route('/validate-ad', methods=['GET', 'POST'])
@admin_required
def validate_ad():
    """Page for AD validation and discrepancy detection"""
    if request.method == 'GET':
        return render_template('admin/validate_ad.html', 
                             title='Validación contra Active Directory')
    
    # Handle POST requests for running validations
    try:
        from app.services.ldap_service import LDAPService
        
        validation_type = request.form.get('validation_type', 'folders')
        target_id = request.form.get('target_id')  # Optional specific folder/user ID
        
        ldap_service = LDAPService()
        
        if validation_type == 'folders':
            folder_id = int(target_id) if target_id and target_id.isdigit() else None
            results = ldap_service.validate_folder_permissions(folder_id)
        elif validation_type == 'users':
            user_id = int(target_id) if target_id and target_id.isdigit() else None
            results = ldap_service.validate_user_groups(user_id)
        else:
            flash('Tipo de validación no válido', 'error')
            return redirect(url_for('admin.validate_ad'))
        
        if results['success']:
            # Log audit event
            AuditEvent.log_event(
                user=current_user,
                event_type='ad_validation',
                action=f'validate_{validation_type}',
                resource_type='system',
                resource_id=None,
                description=f'Validación AD ejecutada para {validation_type}',
                metadata={
                    'validation_type': validation_type,
                    'target_id': target_id,
                    'validated_count': results.get('validated_folders', results.get('validated_users', 0)),
                    'discrepancies_found': results['summary'].get('total_discrepancies', 0)
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            if results['summary'].get('total_discrepancies', 0) > 0:
                flash(f'Validación completada. Se encontraron {results["summary"]["total_discrepancies"]} discrepancias.', 'warning')
            else:
                flash('Validación completada. No se encontraron discrepancias.', 'success')
        else:
            flash(f'Error en la validación: {results.get("error", "Error desconocido")}', 'error')
        
        return render_template('admin/validate_ad.html', 
                             title='Validación contra Active Directory',
                             validation_results=results)
    
    except Exception as e:
        logger.error(f"Error in AD validation: {str(e)}")
        flash(f'Error interno al ejecutar la validación: {str(e)}', 'error')
        return redirect(url_for('admin.validate_ad'))

@admin_bp.route('/validate-ad/api', methods=['POST'])
@admin_required
def validate_ad_api():
    """API endpoint for AD validation (for AJAX calls)"""
    try:
        from app.services.ldap_service import LDAPService
        
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'Datos JSON requeridos'
            }), 400
        
        validation_type = data.get('validation_type', 'folders')
        target_id = data.get('target_id')
        
        ldap_service = LDAPService()
        
        if validation_type == 'folders':
            folder_id = int(target_id) if target_id and str(target_id).isdigit() else None
            results = ldap_service.validate_folder_permissions(folder_id)
        elif validation_type == 'users':
            user_id = int(target_id) if target_id and str(target_id).isdigit() else None
            results = ldap_service.validate_user_groups(user_id)
        else:
            return jsonify({
                'success': False,
                'error': 'Tipo de validación no válido'
            }), 400
        
        # Log audit event for API calls
        if results['success']:
            AuditEvent.log_event(
                user=current_user,
                event_type='ad_validation',
                action=f'api_validate_{validation_type}',
                resource_type='system',
                resource_id=None,
                description=f'Validación AD (API) ejecutada para {validation_type}',
                metadata={
                    'validation_type': validation_type,
                    'target_id': target_id,
                    'validated_count': results.get('validated_folders', results.get('validated_users', 0)),
                    'discrepancies_found': results['summary'].get('total_discrepancies', 0)
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error in AD validation API: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@admin_bp.route('/validate-ad/folder/<int:folder_id>')
@admin_required
def validate_single_folder(folder_id):
    """Validate a specific folder against AD"""
    try:
        from app.services.ldap_service import LDAPService
        from app.models import Folder
        
        folder = Folder.query.get_or_404(folder_id)
        ldap_service = LDAPService()
        
        results = ldap_service.validate_folder_permissions(folder_id)
        
        if results['success']:
            AuditEvent.log_event(
                user=current_user,
                event_type='ad_validation',
                action='validate_single_folder',
                resource_type='folder',
                resource_id=folder_id,
                description=f'Validación AD ejecutada para carpeta {folder.path}',
                metadata={
                    'folder_path': folder.path,
                    'discrepancies_found': results['summary'].get('total_discrepancies', 0)
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        
        return jsonify(results)
    
    except Exception as e:
        logger.error(f"Error validating folder {folder_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@admin_bp.route('/folders/<int:folder_id>/get-ad-permissions')
@login_required
@admin_required
def get_folder_ad_permissions(folder_id):
    """Get AD permissions for a specific folder"""
    try:
        from app.services.ldap_service import LDAPService
        from app.models import Folder
        import logging
        
        logger = logging.getLogger(__name__)
        folder = Folder.query.get_or_404(folder_id)
        ldap_service = LDAPService()
        
        # Get current permissions from database
        db_permissions = []
        for permission in folder.permissions:
            if permission.is_active:
                db_permissions.append({
                    'group_name': permission.ad_group.name,
                    'group_dn': permission.ad_group.distinguished_name,
                    'permission_type': permission.permission_type,
                    'granted_at': permission.granted_at.strftime('%Y-%m-%d %H:%M:%S') if permission.granted_at else None,
                    'granted_by': permission.granted_by.username if permission.granted_by else None
                })
        
        # Try to validate against AD (this will show discrepancies)
        validation_results = ldap_service.validate_folder_permissions(folder_id)
        
        # Get AD group existence validation
        ad_groups_status = []
        conn = ldap_service.get_connection()
        if conn:
            for permission in folder.permissions:
                if permission.is_active:
                    group_exists = ldap_service.verify_group_exists(permission.ad_group.name)
                    ad_groups_status.append({
                        'group_name': permission.ad_group.name,
                        'exists_in_ad': group_exists,
                        'permission_type': permission.permission_type
                    })
            conn.unbind()
        
        results = {
            'success': True,
            'folder': {
                'id': folder.id,
                'name': folder.name,
                'path': folder.path
            },
            'database_permissions': db_permissions,
            'ad_groups_status': ad_groups_status,
            'validation_results': validation_results,
            'summary': {
                'total_permissions_in_db': len(db_permissions),
                'groups_verified_in_ad': len([g for g in ad_groups_status if g['exists_in_ad']]),
                'groups_not_found_in_ad': len([g for g in ad_groups_status if not g['exists_in_ad']])
            }
        }
        
        # Log this action
        AuditEvent.log_event(
            user=current_user,
            event_type='ad_query',
            action='get_folder_ad_permissions',
            resource_type='folder',
            resource_id=folder_id,
            description=f'Consulta de permisos AD para carpeta {folder.path}',
            metadata={
                'folder_path': folder.path,
                'permissions_checked': len(db_permissions),
                'groups_verified': len([g for g in ad_groups_status if g['exists_in_ad']])
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Error getting AD permissions for folder {folder_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'folder_id': folder_id
        }), 500

@admin_bp.route('/folders/sync-users-from-ad', methods=['POST'])
@login_required
@admin_required
def sync_users_from_ad():
    """Start background sync task for complete user synchronization"""
    import logging
    import os
    
    logger = logging.getLogger(__name__)
    
    try:
        from celery_worker import sync_users_from_ad_task
        logger.info("🚀 Starting complete background sync task via Celery")
        
        # Start background task (no parameters - uses env config)
        task = sync_users_from_ad_task.delay(user_id=current_user.id)
        
        logger.info(f"✅ Background sync task started with ID: {task.id}")
        
        # Get configuration for display
        max_folders = int(os.getenv('BACKGROUND_SYNC_MAX_FOLDERS', 50))
        max_members_per_group = int(os.getenv('BACKGROUND_SYNC_MAX_MEMBERS_PER_GROUP', 200))
        enable_full_sync = os.getenv('BACKGROUND_SYNC_ENABLE_FULL_SYNC', 'true').lower() == 'true'
        
        # Log this action
        AuditEvent.log_event(
            user=current_user,
            event_type='ad_sync',
            action='sync_users_from_ad_background_started',
            resource_type='system',
            description=f'Sincronización completa en background iniciada - Task ID: {task.id}',
            metadata={
                'task_id': task.id,
                'max_folders': max_folders,
                'max_members_per_group': max_members_per_group,
                'full_sync_enabled': enable_full_sync,
                'background_task': True
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': 'Sincronización completa iniciada en background',
            'task_id': task.id,
            'status': 'STARTED',
            'background_task': True,
            'configuration': {
                'max_folders': max_folders,
                'max_members_per_group': max_members_per_group,
                'full_sync_enabled': enable_full_sync
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Error starting background sync task: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Error iniciando tarea en background: {str(e)}'
        }), 500

@admin_bp.route('/folders/sync-task-status/<task_id>', methods=['GET'])
@login_required
@admin_required
def get_sync_task_status(task_id):
    """Get status of background sync task"""
    try:
        from celery_worker import sync_users_from_ad_task
        from celery.result import AsyncResult
        import logging
        
        logger = logging.getLogger(__name__)
        
        # Get task result
        task_result = AsyncResult(task_id)
        
        response = {
            'task_id': task_id,
            'status': task_result.state,
            'current': 0,
            'total': 0,
            'result': None
        }
        
        if task_result.state == 'PENDING':
            response.update({
                'status': 'PENDING',
                'current': 0,
                'total': 1,
                'message': 'Tarea en cola, esperando...'
            })
        elif task_result.state == 'PROGRESS':
            response.update({
                'status': 'PROGRESS',
                'current': task_result.info.get('current', 0),
                'total': task_result.info.get('total', 1),
                'message': task_result.info.get('message', 'Procesando...')
            })
        elif task_result.state == 'SUCCESS':
            response.update({
                'status': 'SUCCESS',
                'current': 1,
                'total': 1,
                'result': task_result.result,
                'message': 'Sincronización completada exitosamente'
            })
        else:  # FAILURE
            response.update({
                'status': 'FAILURE',
                'current': 1,
                'total': 1,
                'error': str(task_result.info),
                'message': f'Error en sincronización: {str(task_result.info)}'
            })
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"❌ Error getting task status: {str(e)}")
        return jsonify({
            'task_id': task_id,
            'status': 'ERROR',
            'error': f'Error obteniendo estado de la tarea: {str(e)}'
        }), 500


@admin_bp.route('/folders/sync-users-from-ad-old', methods=['POST'])
@login_required
@admin_required
def sync_users_from_ad_old():
    """Sync users with permissions from AD for all folders (OPTIMIZED FOR LARGE DATASETS)"""
    try:
        from app.services.ldap_service import LDAPService
        from app.models import Folder, User, FolderPermission, UserADGroupMembership, ADGroup
        import logging
        import ldap3
        
        logger = logging.getLogger(__name__)
        logger.info("🚀 Starting OPTIMIZED user sync from AD")
        
        ldap_service = LDAPService()
        
        results = {
            'success': True,
            'folders_processed': 0,
            'users_synced': 0,
            'memberships_created': 0,
            'errors': [],
            'summary': {},
            'skipped_large_groups': 0
        }
        
        # OPTIMIZATION 1: Ultra-aggressive limits to prevent timeout
        max_folders = min(int(request.form.get('max_folders', 5)), 5)  # Hard limit to 5 folders max
        max_members_per_group = min(int(request.form.get('max_members', 20)), 20)  # Hard limit to 20 members max
        
        folders = Folder.query.filter_by(is_active=True).limit(max_folders).all()
        
        logger.info(f"🚀 ULTRA-OPTIMIZED sync: {len(folders)} folders (hard limit: {max_folders})")
        
        conn = ldap_service.get_connection()
        if not conn:
            return jsonify({
                'success': False,
                'error': 'No se pudo conectar a LDAP'
            }), 500
        
        # OPTIMIZATION 2: Pre-cache all users to avoid repeated LDAP queries
        logger.info("📋 Pre-caching existing users to avoid duplicate lookups...")
        existing_users = {}
        for user in User.query.all():
            if user.username:
                existing_users[user.username.lower()] = user
        logger.info(f"💾 Cached {len(existing_users)} existing users")
        
        for folder in folders:
            try:
                folder_users_synced = 0
                folder_memberships_created = 0
                logger.info(f"=== Processing folder: {folder.name} (ID: {folder.id}) ===")
                
                # Get all active permissions for this folder
                active_permissions = [fp for fp in folder.permissions if fp.is_active]
                logger.info(f"Found {len(active_permissions)} active permissions for folder {folder.name}")
                
                if not active_permissions:
                    logger.warning(f"No active permissions found for folder {folder.name}")
                    continue
                
                for permission in active_permissions:
                    ad_group = permission.ad_group
                    logger.info(f"Processing group {ad_group.name} for folder {folder.name}")
                    
                    # OPTIMIZATION 3: Get group members but with strict limits
                    try:
                        group_members = ldap_service.get_group_members(ad_group.distinguished_name)
                        logger.info(f"Found {len(group_members)} members in group {ad_group.name}")
                    except Exception as group_error:
                        logger.error(f"❌ Failed to get members for group {ad_group.name}: {str(group_error)}")
                        results['errors'].append(f"Error obteniendo miembros del grupo {ad_group.name}: {str(group_error)}")
                        continue
                    
                    if not group_members:
                        logger.warning(f"No members found for group {ad_group.name}")
                        continue
                    
                    # OPTIMIZATION 4: Ultra-strict member limits
                    if len(group_members) > max_members_per_group:
                        logger.warning(f"⚠️ Skipping large group {ad_group.name} with {len(group_members)} members (ultra-strict limit: {max_members_per_group})")
                        results['skipped_large_groups'] += 1
                        continue
                    
                    # OPTIMIZATION 5: Process only first N members with immediate commits
                    processed_members = 0
                    max_process_per_group = 10  # Even stricter limit
                    
                    for i, member_dn in enumerate(group_members):
                        if processed_members >= max_process_per_group:
                            logger.warning(f"⏰ Stopping at {max_process_per_group} members for group {ad_group.name} (timeout prevention)")
                            break
                        try:
                            logger.debug(f"Processing member DN: {member_dn}")
                            
                            # Instead of trying to extract username from DN, 
                            # search directly by DN to get the actual user details
                            user_found = False
                            sam_account = None
                            full_name = None
                            email = None
                            department = None
                            
                            # Direct search by DN to get user details
                            try:
                                search_filter = f"(distinguishedName={member_dn})"
                                attributes = [
                                    'cn', 'sAMAccountName', 'displayName', 'mail', 
                                    'department', 'givenName', 'sn', 'distinguishedName'
                                ]
                                
                                logger.debug(f"Searching user with filter: {search_filter}")
                                
                                conn.search(
                                    search_base=ldap_service.base_dn,
                                    search_filter=search_filter,
                                    attributes=attributes,
                                    search_scope=ldap3.SUBTREE
                                )
                                
                                if conn.entries:
                                    user_entry = conn.entries[0]
                                    sam_account = str(user_entry.sAMAccountName) if user_entry.sAMAccountName else None
                                    full_name = str(user_entry.displayName) if user_entry.displayName else str(user_entry.cn) if user_entry.cn else None
                                    email = str(user_entry.mail) if user_entry.mail else None
                                    department = str(user_entry.department) if user_entry.department else None
                                    user_found = True
                                
                                if not user_found or not sam_account:
                                    # If DN search failed, try extracting username from DN and search by sAMAccountName
                                    extracted_username = None
                                    member_dn_lower = member_dn.lower()
                                    
                                    if 'cn=' in member_dn_lower:
                                        extracted_username = member_dn.split('cn=')[1].split(',')[0].strip()
                                    elif 'uid=' in member_dn_lower:
                                        extracted_username = member_dn.split('uid=')[1].split(',')[0].strip()
                                    
                                    if extracted_username:
                                        logger.debug(f"Trying search by extracted username: {extracted_username}")
                                        search_filter_sam = f"(sAMAccountName={extracted_username})"
                                        conn.search(
                                            search_base=ldap_service.base_dn,
                                            search_filter=search_filter_sam,
                                            attributes=attributes,
                                            search_scope=ldap3.SUBTREE
                                        )
                                        
                                        if conn.entries:
                                            user_entry = conn.entries[0]
                                            sam_account = str(user_entry.sAMAccountName) if user_entry.sAMAccountName else extracted_username
                                            full_name = str(user_entry.displayName) if user_entry.displayName else str(user_entry.cn) if user_entry.cn else extracted_username
                                            email = str(user_entry.mail) if user_entry.mail else f"{sam_account}@example.org"
                                            department = str(user_entry.department) if user_entry.department else None
                                            user_found = True
                                
                                if user_found and sam_account:
                                    logger.debug(f"User found in AD - sAMAccountName: {sam_account}, displayName: {full_name}")
                                    
                                    # Find or create user in database
                                    user = User.query.filter_by(username=sam_account.lower()).first()
                                    if not user:
                                        logger.info(f"Creating new user: {sam_account.lower()}")
                                        user = User(
                                            username=sam_account.lower(),
                                            email=email or f"{sam_account}@example.org",
                                            full_name=full_name or sam_account,
                                            department=department,
                                            distinguished_name=member_dn,
                                            is_active=True
                                        )
                                        db.session.add(user)
                                        db.session.flush()  # Get user ID
                                        folder_users_synced += 1
                                    else:
                                        logger.debug(f"Updating existing user: {sam_account.lower()}")
                                        # Update existing user information
                                        user.full_name = full_name or user.full_name
                                        user.email = email or user.email
                                        user.department = department or user.department
                                        user.distinguished_name = member_dn
                                
                                    # Check if membership already exists
                                    existing_membership = UserADGroupMembership.query.filter_by(
                                        user_id=user.id,
                                        ad_group_id=ad_group.id
                                    ).first()
                                    
                                    if not existing_membership:
                                        logger.info(f"Creating membership: user {user.username} -> group {ad_group.name}")
                                        # Create AD group membership
                                        membership = UserADGroupMembership(
                                            user_id=user.id,
                                            ad_group_id=ad_group.id,
                                            granted_by_id=current_user.id,
                                            is_active=True,
                                            notes=f'Sincronizado desde AD para carpeta {folder.name}'
                                        )
                                        db.session.add(membership)
                                        folder_memberships_created += 1
                                    elif not existing_membership.is_active:
                                        logger.info(f"Reactivating membership: user {user.username} -> group {ad_group.name}")
                                        # Reactivate existing membership
                                        existing_membership.is_active = True
                                        existing_membership.granted_by_id = current_user.id
                                        existing_membership.notes = f'Reactivado desde AD para carpeta {folder.name}'
                                        folder_memberships_created += 1
                                    else:
                                        logger.debug(f"Membership already active: user {user.username} -> group {ad_group.name}")
                                else:
                                    logger.warning(f"User not found in AD for DN: {member_dn}")
                                    results['errors'].append(f"Usuario no encontrado en AD (DN: {member_dn})")
                                
                            except Exception as user_search_error:
                                logger.warning(f"Error searching for user {member_dn}: {str(user_search_error)}")
                                results['errors'].append(f"Error buscando usuario {member_dn}: {str(user_search_error)}")
                                continue
                        
                        except Exception as e:
                            logger.warning(f"Error processing member {member_dn}: {str(e)}")
                            continue
                        
                        # OPTIMIZATION 4: Commit in batches to avoid long transactions
                        processed_in_batch += 1
                        if processed_in_batch >= batch_size or i == len(group_members) - 1:
                            try:
                                db.session.commit()
                                logger.debug(f"✅ Batch committed: {processed_in_batch} users processed")
                                processed_in_batch = 0
                            except Exception as commit_error:
                                logger.error(f"❌ Batch commit failed: {str(commit_error)}")
                                db.session.rollback()
                                results['errors'].append(f"Error en commit: {str(commit_error)}")
                
                results['folders_processed'] += 1
                results['users_synced'] += folder_users_synced
                results['memberships_created'] += folder_memberships_created
                
            except Exception as e:
                logger.error(f"Error processing folder {folder.id}: {str(e)}")
                results['errors'].append(f"Carpeta {folder.name}: {str(e)}")
                continue
        
        # Final commit for any remaining changes
        try:
            db.session.commit()
        except Exception as final_commit_error:
            logger.error(f"❌ Final commit failed: {str(final_commit_error)}")
            db.session.rollback()
            results['errors'].append(f"Error en commit final: {str(final_commit_error)}")
        
        conn.unbind()
        
        # Create summary with optimizations info
        results['summary'] = {
            'total_folders_in_system': Folder.query.filter_by(is_active=True).count(),
            'folders_processed': results['folders_processed'],
            'users_synced': results['users_synced'],
            'memberships_created': results['memberships_created'],
            'errors_count': len(results['errors']),
            'skipped_large_groups': results['skipped_large_groups'],
            'optimizations_applied': {
                'folder_limit': max_folders,
                'member_limit_per_group': max_members_per_group,
                'batch_processing': True,
                'timeout_prevention': True
            }
        }
        
        logger.info(f"🎉 OPTIMIZED sync completed: {results['folders_processed']}/{max_folders} folders, {results['users_synced']} users, {results['skipped_large_groups']} large groups skipped")
        
        # Log this action
        AuditEvent.log_event(
            user=current_user,
            event_type='ad_sync',
            action='sync_users_from_ad',
            resource_type='system',
            description=f'Sincronización masiva optimizada de usuarios desde AD (limitada a {max_folders} carpetas)',
            metadata={
                'folders_processed': results['folders_processed'],
                'users_synced': results['users_synced'],
                'memberships_created': results['memberships_created'],
                'errors_count': len(results['errors']),
                'skipped_large_groups': results['skipped_large_groups'],
                'optimization_settings': {
                    'max_folders': max_folders,
                    'max_members_per_group': max_members_per_group,
                    'batch_processing': True
                }
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify(results)
        
    except Exception as e:
        db.session.rollback()
        
        # Restore logging levels even in case of error
        try:
            logger.setLevel(original_level)
            ldap_logger.setLevel(ldap_original_level)
        except:
            pass
            
        logger.error(f"Error in sync_users_from_ad: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@admin_bp.route('/debug/sync-analysis/<folder_name>', methods=['GET'])
@admin_required
def debug_sync_analysis(folder_name):
    """Debug sync process for a specific folder"""
    try:
        from app.services.ldap_service import LDAPService
        from app.models import Folder, User, FolderPermission, UserADGroupMembership, ADGroup
        import ldap3
        
        ldap_service = LDAPService()
        
        analysis = {
            'folder_name': folder_name,
            'folder_in_db': False,
            'folder_details': None,
            'permissions': [],
            'ad_groups': [],
            'sync_issues': []
        }
        
        # Check if folder exists in database
        folder = Folder.query.filter_by(name=folder_name).first()
        if not folder:
            analysis['sync_issues'].append(f"Carpeta '{folder_name}' no existe en la base de datos")
            return jsonify(analysis)
        
        analysis['folder_in_db'] = True
        analysis['folder_details'] = {
            'id': folder.id,
            'name': folder.name,
            'path': folder.path,
            'is_active': folder.is_active
        }
        
        # Get folder permissions
        permissions = FolderPermission.query.filter_by(folder_id=folder.id, is_active=True).all()
        analysis['permissions'] = []
        
        conn = ldap_service.get_connection()
        if not conn:
            analysis['sync_issues'].append("No se pudo conectar a LDAP")
            return jsonify(analysis)
        
        for permission in permissions:
            ad_group = permission.ad_group
            perm_analysis = {
                'permission_id': permission.id,
                'permission_type': permission.permission_type,
                'ad_group': {
                    'id': ad_group.id,
                    'name': ad_group.name,
                    'dn': ad_group.distinguished_name,
                    'is_active': ad_group.is_active
                },
                'group_exists_in_ad': False,
                'members_in_ad': [],
                'members_in_db': [],
                'issues': []
            }
            
            # Check if group exists in AD
            try:
                group_exists = ldap_service.verify_group_exists(ad_group.name)
                perm_analysis['group_exists_in_ad'] = group_exists
                
                if not group_exists:
                    perm_analysis['issues'].append(f"Grupo '{ad_group.name}' no existe en AD")
                else:
                    # Get members from AD
                    ad_members = ldap_service.get_group_members(ad_group.distinguished_name)
                    perm_analysis['members_in_ad'] = ad_members
                    
                    if not ad_members:
                        perm_analysis['issues'].append(f"Grupo '{ad_group.name}' no tiene miembros en AD")
                    
                    # For each AD member, try to resolve user info
                    resolved_members = []
                    for member_dn in ad_members:
                        member_info = {
                            'dn': member_dn,
                            'username': None,
                            'found_in_ad': False,
                            'found_in_db': False,
                            'user_details': None
                        }
                        
                        # Extract username from DN
                        if 'cn=' in member_dn.lower():
                            member_info['username'] = member_dn.split('cn=')[1].split(',')[0].strip()
                        elif 'uid=' in member_dn.lower():
                            member_info['username'] = member_dn.split('uid=')[1].split(',')[0].strip()
                        
                        if member_info['username']:
                            # Search user in AD
                            try:
                                search_filter = f"(distinguishedName={member_dn})"
                                attributes = ['cn', 'sAMAccountName', 'displayName', 'mail']
                                
                                conn.search(
                                    search_base=ldap_service.base_dn,
                                    search_filter=search_filter,
                                    attributes=attributes,
                                    search_scope=ldap3.SUBTREE
                                )
                                
                                if conn.entries:
                                    user_entry = conn.entries[0]
                                    member_info['found_in_ad'] = True
                                    member_info['user_details'] = {
                                        'sAMAccountName': str(user_entry.sAMAccountName) if user_entry.sAMAccountName else None,
                                        'displayName': str(user_entry.displayName) if user_entry.displayName else None,
                                        'mail': str(user_entry.mail) if user_entry.mail else None
                                    }
                                    
                                    # Check if user exists in database
                                    sam_account = member_info['user_details']['sAMAccountName'] or member_info['username']
                                    db_user = User.query.filter_by(username=sam_account.lower()).first()
                                    if db_user:
                                        member_info['found_in_db'] = True
                                        member_info['db_user_id'] = db_user.id
                                        
                                        # Check if membership exists
                                        membership = UserADGroupMembership.query.filter_by(
                                            user_id=db_user.id,
                                            ad_group_id=ad_group.id
                                        ).first()
                                        member_info['membership_exists'] = membership is not None
                                        member_info['membership_active'] = membership.is_active if membership else False
                                    
                            except Exception as e:
                                member_info['search_error'] = str(e)
                        
                        resolved_members.append(member_info)
                    
                    perm_analysis['resolved_members'] = resolved_members
                    
            except Exception as e:
                perm_analysis['issues'].append(f"Error verificando grupo en AD: {str(e)}")
            
            analysis['permissions'].append(perm_analysis)
        
        conn.unbind()
        return jsonify(analysis)
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'folder_name': folder_name
        }), 500

@admin_bp.route('/debug/compare-ad-vs-db', methods=['GET'])
@admin_required
def compare_ad_vs_db():
    """Compare what's in AD vs what's in DB for all folders"""
    try:
        from app.services.ldap_service import LDAPService
        from app.models import Folder, User, FolderPermission, UserADGroupMembership, ADGroup
        import ldap3
        
        ldap_service = LDAPService()
        
        comparison = {
            'folders_analyzed': 0,
            'discrepancies_found': [],
            'summary': {
                'users_in_ad_not_in_db': 0,
                'users_in_db_not_in_ad': 0,
                'inactive_memberships': 0,
                'sync_needed': False
            }
        }
        
        conn = ldap_service.get_connection()
        if not conn:
            return jsonify({'error': 'No se pudo conectar a LDAP'}), 500
        
        folders = Folder.query.filter_by(is_active=True).all()
        
        for folder in folders:
            folder_discrepancies = {
                'folder_name': folder.name,
                'folder_id': folder.id,
                'permissions_analyzed': 0,
                'issues': []
            }
            
            active_permissions = [fp for fp in folder.permissions if fp.is_active]
            folder_discrepancies['permissions_analyzed'] = len(active_permissions)
            
            for permission in active_permissions:
                ad_group = permission.ad_group
                
                try:
                    # Get members from AD
                    ad_members = ldap_service.get_group_members(ad_group.distinguished_name)
                    
                    # Get current memberships from DB
                    db_memberships = UserADGroupMembership.query.filter_by(
                        ad_group_id=ad_group.id,
                        is_active=True
                    ).all()
                    
                    # Create sets for comparison
                    ad_usernames = set()
                    db_usernames = set()
                    
                    # Process AD members
                    for member_dn in ad_members:
                        try:
                            # Search user in AD
                            search_filter = f"(distinguishedName={member_dn})"
                            conn.search(
                                search_base=ldap_service.base_dn,
                                search_filter=search_filter,
                                attributes=['sAMAccountName', 'cn'],
                                search_scope=ldap3.SUBTREE
                            )
                            
                            if conn.entries:
                                user_entry = conn.entries[0]
                                sam_account = str(user_entry.sAMAccountName) if user_entry.sAMAccountName else None
                                if sam_account:
                                    ad_usernames.add(sam_account.lower())
                        except Exception as e:
                            folder_discrepancies['issues'].append(f"Error processing AD member {member_dn}: {str(e)}")
                    
                    # Process DB memberships
                    for membership in db_memberships:
                        db_usernames.add(membership.user.username.lower())
                    
                    # Find discrepancies
                    users_in_ad_not_db = ad_usernames - db_usernames
                    users_in_db_not_ad = db_usernames - ad_usernames
                    
                    if users_in_ad_not_db:
                        folder_discrepancies['issues'].append({
                            'type': 'users_in_ad_not_db',
                            'group': ad_group.name,
                            'permission_type': permission.permission_type,
                            'users': list(users_in_ad_not_db),
                            'description': f"Usuarios en AD pero no en BD para grupo {ad_group.name}"
                        })
                        comparison['summary']['users_in_ad_not_in_db'] += len(users_in_ad_not_db)
                        comparison['summary']['sync_needed'] = True
                    
                    if users_in_db_not_ad:
                        folder_discrepancies['issues'].append({
                            'type': 'users_in_db_not_ad',
                            'group': ad_group.name,
                            'permission_type': permission.permission_type,
                            'users': list(users_in_db_not_ad),
                            'description': f"Usuarios en BD pero no en AD para grupo {ad_group.name}"
                        })
                        comparison['summary']['users_in_db_not_in_ad'] += len(users_in_db_not_ad)
                        comparison['summary']['sync_needed'] = True
                    
                except Exception as e:
                    folder_discrepancies['issues'].append(f"Error processing group {ad_group.name}: {str(e)}")
            
            if folder_discrepancies['issues']:
                comparison['discrepancies_found'].append(folder_discrepancies)
            
            comparison['folders_analyzed'] += 1
        
        conn.unbind()
        return jsonify(comparison)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/debug/force-sync-discrepancies', methods=['POST'])
@admin_required
def force_sync_discrepancies():
    """Force sync for users that are in AD but not in DB"""
    try:
        from app.services.ldap_service import LDAPService
        from app.models import Folder, User, FolderPermission, UserADGroupMembership, ADGroup
        import ldap3
        import logging
        
        ldap_service = LDAPService()
        
        # Enable debug logging
        logger = logging.getLogger(__name__)
        original_level = logger.level
        logger.setLevel(logging.DEBUG)
        
        results = {
            'success': True,
            'users_processed': 0,
            'memberships_created': 0,
            'errors': [],
            'details': []
        }
        
        conn = ldap_service.get_connection()
        if not conn:
            return jsonify({'error': 'No se pudo conectar a LDAP'}), 500
        
        # Process specific known discrepancies
        discrepancies = [
            {'folder': 'Carpeta Proyectos', 'group': 'proyectos europeos_R', 'users': ['laia.perez']},
            {'folder': 'Carpeta Compras', 'group': 'compras_R', 'users': ['admin']},
            {'folder': 'Carpeta Compras', 'group': 'compras_W', 'users': ['admin']}
        ]
        
        for disc in discrepancies:
            folder = Folder.query.filter_by(name=disc['folder']).first()
            if not folder:
                results['errors'].append(f"Folder '{disc['folder']}' not found")
                continue
            
            ad_group = ADGroup.query.filter_by(name=disc['group']).first()
            if not ad_group:
                results['errors'].append(f"AD Group '{disc['group']}' not found")
                continue
            
            logger.info(f"Processing discrepancy for folder {folder.name}, group {ad_group.name}")
            
            for username in disc['users']:
                try:
                    user_detail = {
                        'username': username,
                        'folder': folder.name,
                        'group': ad_group.name,
                        'processed': False,
                        'created_user': False,
                        'created_membership': False,
                        'error': None
                    }
                    
                    logger.info(f"Processing user: {username}")
                    
                    # Search user in AD by sAMAccountName
                    search_filter = f"(sAMAccountName={username})"
                    attributes = ['cn', 'sAMAccountName', 'displayName', 'mail', 'department', 'distinguishedName']
                    
                    conn.search(
                        search_base=ldap_service.base_dn,
                        search_filter=search_filter,
                        attributes=attributes,
                        search_scope=ldap3.SUBTREE
                    )
                    
                    if not conn.entries:
                        user_detail['error'] = f"User {username} not found in AD"
                        results['errors'].append(user_detail['error'])
                        results['details'].append(user_detail)
                        continue
                    
                    user_entry = conn.entries[0]
                    logger.info(f"Found user in AD: {username}")
                    
                    # Extract user information
                    sam_account = str(user_entry.sAMAccountName) if user_entry.sAMAccountName else username
                    full_name = str(user_entry.displayName) if user_entry.displayName else str(user_entry.cn) if user_entry.cn else username
                    email = str(user_entry.mail) if user_entry.mail else f"{sam_account}@example.org"
                    department = str(user_entry.department) if user_entry.department else None
                    dn = str(user_entry.distinguishedName) if user_entry.distinguishedName else None
                    
                    # Find or create user in database
                    user = User.query.filter_by(username=sam_account.lower()).first()
                    if not user:
                        logger.info(f"Creating new user: {sam_account}")
                        user = User(
                            username=sam_account.lower(),
                            email=email,
                            full_name=full_name,
                            department=department,
                            distinguished_name=dn,
                            is_active=True
                        )
                        db.session.add(user)
                        db.session.flush()  # Get user ID
                        user_detail['created_user'] = True
                        results['users_processed'] += 1
                    else:
                        logger.info(f"User already exists: {sam_account}")
                        # Update existing user information
                        user.full_name = full_name
                        user.email = email
                        user.department = department
                        user.distinguished_name = dn
                    
                    # Check if membership already exists
                    existing_membership = UserADGroupMembership.query.filter_by(
                        user_id=user.id,
                        ad_group_id=ad_group.id
                    ).first()
                    
                    if not existing_membership:
                        logger.info(f"Creating membership: {username} -> {ad_group.name}")
                        membership = UserADGroupMembership(
                            user_id=user.id,
                            ad_group_id=ad_group.id,
                            granted_by_id=current_user.id,
                            is_active=True,
                            notes=f'Sincronización forzada para resolver discrepancia'
                        )
                        db.session.add(membership)
                        user_detail['created_membership'] = True
                        results['memberships_created'] += 1
                    elif not existing_membership.is_active:
                        logger.info(f"Reactivating membership: {username} -> {ad_group.name}")
                        existing_membership.is_active = True
                        existing_membership.granted_by_id = current_user.id
                        existing_membership.notes = f'Reactivado por sincronización forzada'
                        user_detail['created_membership'] = True
                        results['memberships_created'] += 1
                    else:
                        logger.info(f"Membership already active: {username} -> {ad_group.name}")
                    
                    user_detail['processed'] = True
                    results['details'].append(user_detail)
                    
                except Exception as e:
                    error_msg = f"Error processing user {username}: {str(e)}"
                    logger.error(error_msg)
                    user_detail['error'] = error_msg
                    results['errors'].append(error_msg)
                    results['details'].append(user_detail)
        
        # Commit all changes
        db.session.commit()
        conn.unbind()
        
        # Restore logging level
        logger.setLevel(original_level)
        
        logger.info(f"Force sync completed: {results['users_processed']} users, {results['memberships_created']} memberships")
        
        return jsonify(results)
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@admin_bp.route('/debug/permissions-data', methods=['GET'])
@admin_required
def debug_permissions_data():
    """Debug endpoint to check what data exists for permissions report"""
    try:
        from app.models import PermissionRequest, UserADGroupMembership, FolderPermission, Folder, ADGroup, User
        
        debug_info = {
            'permission_requests': {
                'total': 0,
                'by_status': {},
                'approved': []
            },
            'ad_memberships': {
                'total': 0,
                'active': 0,
                'sample': []
            },
            'folders': {
                'total': 0,
                'active': 0,
                'sample': []
            },
            'folder_permissions': {
                'total': 0,
                'active': 0,
                'sample': []
            },
            'users': {
                'total': 0,
                'sample': []
            },
            'ad_groups': {
                'total': 0,
                'sample': []
            }
        }
        
        # Check Permission Requests
        all_requests = PermissionRequest.query.all()
        debug_info['permission_requests']['total'] = len(all_requests)
        
        status_count = {}
        for req in all_requests:
            status = req.status
            status_count[status] = status_count.get(status, 0) + 1
            
            if req.status == 'approved':
                debug_info['permission_requests']['approved'].append({
                    'id': req.id,
                    'requester': req.requester.username if req.requester else 'None',
                    'folder': req.folder.name if req.folder else 'None',
                    'permission_type': req.permission_type,
                    'status': req.status
                })
        
        debug_info['permission_requests']['by_status'] = status_count
        
        # Check AD Memberships
        all_memberships = UserADGroupMembership.query.all()
        debug_info['ad_memberships']['total'] = len(all_memberships)
        debug_info['ad_memberships']['active'] = len([m for m in all_memberships if m.is_active])
        
        for i, membership in enumerate(all_memberships[:5]):  # First 5
            debug_info['ad_memberships']['sample'].append({
                'id': membership.id,
                'user': membership.user.username if membership.user else 'None',
                'ad_group': membership.ad_group.name if membership.ad_group else 'None',
                'is_active': membership.is_active
            })
        
        # Check Folders
        all_folders = Folder.query.all()
        debug_info['folders']['total'] = len(all_folders)
        debug_info['folders']['active'] = len([f for f in all_folders if f.is_active])
        
        for folder in all_folders[:5]:  # First 5
            debug_info['folders']['sample'].append({
                'id': folder.id,
                'name': folder.name,
                'path': folder.path,
                'is_active': folder.is_active
            })
        
        # Check Folder Permissions
        all_folder_perms = FolderPermission.query.all()
        debug_info['folder_permissions']['total'] = len(all_folder_perms)
        debug_info['folder_permissions']['active'] = len([fp for fp in all_folder_perms if fp.is_active])
        
        for fp in all_folder_perms[:5]:  # First 5
            debug_info['folder_permissions']['sample'].append({
                'id': fp.id,
                'folder': fp.folder.name if fp.folder else 'None',
                'ad_group': fp.ad_group.name if fp.ad_group else 'None',
                'permission_type': fp.permission_type,
                'is_active': fp.is_active
            })
        
        # Check Users
        all_users = User.query.all()
        debug_info['users']['total'] = len(all_users)
        
        for user in all_users[:5]:  # First 5
            debug_info['users']['sample'].append({
                'id': user.id,
                'username': user.username,
                'full_name': user.full_name,
                'is_active': user.is_active
            })
        
        # Check AD Groups
        all_groups = ADGroup.query.all()
        debug_info['ad_groups']['total'] = len(all_groups)
        
        for group in all_groups[:5]:  # First 5
            debug_info['ad_groups']['sample'].append({
                'id': group.id,
                'name': group.name,
                'is_active': group.is_active
            })
        
        return jsonify(debug_info)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/debug/test-specific-users', methods=['GET'])
@admin_required
def test_specific_users():
    """Test if the specific users we know about are in the system"""
    try:
        from app.models import User, UserADGroupMembership, ADGroup, FolderPermission
        
        test_users = ['laia.perez', 'admin']
        result = {
            'users_checked': test_users,
            'results': {}
        }
        
        for username in test_users:
            user_info = {
                'exists_in_db': False,
                'user_details': None,
                'ad_memberships': [],
                'potential_permissions': []
            }
            
            # Check if user exists
            user = User.query.filter_by(username=username.lower()).first()
            if user:
                user_info['exists_in_db'] = True
                user_info['user_details'] = {
                    'id': user.id,
                    'username': user.username,
                    'full_name': user.full_name,
                    'email': user.email,
                    'is_active': user.is_active
                }
                
                # Check AD memberships
                memberships = UserADGroupMembership.query.filter_by(user_id=user.id).all()
                for membership in memberships:
                    user_info['ad_memberships'].append({
                        'id': membership.id,
                        'ad_group': membership.ad_group.name if membership.ad_group else 'None',
                        'is_active': membership.is_active,
                        'granted_at': str(membership.granted_at) if hasattr(membership, 'granted_at') and membership.granted_at else 'None'
                    })
                    
                    # Check what folder permissions this group gives
                    if membership.ad_group:
                        folder_perms = FolderPermission.query.filter_by(ad_group_id=membership.ad_group.id).all()
                        for fp in folder_perms:
                            user_info['potential_permissions'].append({
                                'folder': fp.folder.name if fp.folder else 'None',
                                'permission_type': fp.permission_type,
                                'is_active': fp.is_active
                            })
            
            result['results'][username] = user_info
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/debug/test-report-logic', methods=['GET'])
@admin_required
def test_report_logic():
    """Test the permissions report logic step by step"""
    try:
        from app.models import PermissionRequest, UserADGroupMembership, FolderPermission, Folder, ADGroup, User
        
        result = {
            'step1_approved_requests': {'count': 0, 'permissions': []},
            'step2_ad_memberships': {'count': 0, 'memberships': []},
            'step3_virtual_permissions': {'count': 0, 'permissions': []},
            'final_permissions_by_user': {}
        }
        
        permissions_by_user = {}
        all_permissions = []
        
        # STEP 1: Get approved permission requests
        try:
            query = PermissionRequest.query.filter_by(status='approved')
            approved_requests = query.options(
                db.joinedload(PermissionRequest.requester),
                db.joinedload(PermissionRequest.folder),
                db.joinedload(PermissionRequest.ad_group)
            ).all()
            
            result['step1_approved_requests']['count'] = len(approved_requests)
            
            for permission in approved_requests:
                if not permission.folder or not permission.folder.is_active:
                    continue
                
                user_id = permission.requester_id
                folder_id_key = permission.folder_id
                
                if user_id not in permissions_by_user:
                    permissions_by_user[user_id] = {
                        'user': permission.requester,
                        'folders': {}
                    }
                
                if folder_id_key not in permissions_by_user[user_id]['folders']:
                    permissions_by_user[user_id]['folders'][folder_id_key] = {
                        'folder': permission.folder,
                        'permissions': []
                    }
                
                permissions_by_user[user_id]['folders'][folder_id_key]['permissions'].append(permission)
                all_permissions.append(permission)
                
                result['step1_approved_requests']['permissions'].append({
                    'user': permission.requester.username,
                    'folder': permission.folder.name,
                    'permission_type': permission.permission_type
                })
                
        except Exception as e:
            result['step1_approved_requests']['error'] = str(e)
        
        # STEP 2: Get AD memberships
        try:
            memberships = UserADGroupMembership.query.filter_by(is_active=True).all()
            result['step2_ad_memberships']['count'] = len(memberships)
            
            for membership in memberships:
                if not membership.user or not membership.ad_group:
                    continue
                
                result['step2_ad_memberships']['memberships'].append({
                    'user': membership.user.username,
                    'ad_group': membership.ad_group.name
                })
                
                # Get folder permissions for this AD group
                folder_perms = FolderPermission.query.filter_by(
                    ad_group_id=membership.ad_group.id,
                    is_active=True
                ).all()
                
                for fp in folder_perms:
                    if not fp.folder or not fp.folder.is_active:
                        continue
                    
                    user_id = membership.user.id
                    folder_id_key = fp.folder.id
                    
                    # Initialize user entry if not exists
                    if user_id not in permissions_by_user:
                        permissions_by_user[user_id] = {
                            'user': membership.user,
                            'folders': {}
                        }
                    
                    # Initialize folder entry if not exists
                    if folder_id_key not in permissions_by_user[user_id]['folders']:
                        permissions_by_user[user_id]['folders'][folder_id_key] = {
                            'folder': fp.folder,
                            'permissions': []
                        }
                    
                    # Check if this permission already exists (avoid duplicates)
                    exists = False
                    for existing in permissions_by_user[user_id]['folders'][folder_id_key]['permissions']:
                        if (hasattr(existing, 'permission_type') and 
                            existing.permission_type == fp.permission_type):
                            exists = True
                            break
                    
                    if not exists:
                        # Create virtual permission
                        class VirtualPermission:
                            def __init__(self, membership, folder_permission):
                                self.id = f"sync_{membership.id}_{folder_permission.id}"
                                self.folder_id = folder_permission.folder.id
                                self.folder = folder_permission.folder
                                self.permission_type = folder_permission.permission_type
                                self.ad_group = membership.ad_group
                                self.validator = None
                                self.validated_at = None
                                self.requester_id = membership.user.id
                                self.requester = membership.user
                                self.source = 'ad_sync'
                        
                        virtual_perm = VirtualPermission(membership, fp)
                        permissions_by_user[user_id]['folders'][folder_id_key]['permissions'].append(virtual_perm)
                        all_permissions.append(virtual_perm)
                        
                        result['step3_virtual_permissions']['permissions'].append({
                            'user': membership.user.username,
                            'folder': fp.folder.name,
                            'permission_type': fp.permission_type,
                            'source': 'ad_sync'
                        })
                        
        except Exception as e:
            result['step2_ad_memberships']['error'] = str(e)
        
        result['step3_virtual_permissions']['count'] = len(result['step3_virtual_permissions']['permissions'])
        
        # Final structure
        for user_id, user_data in permissions_by_user.items():
            result['final_permissions_by_user'][user_data['user'].username] = {
                'folders': len(user_data['folders']),
                'total_permissions': sum(len(folder_data['permissions']) for folder_data in user_data['folders'].values())
            }
        
        result['summary'] = {
            'total_users_with_permissions': len(permissions_by_user),
            'total_permissions': len(all_permissions)
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@admin_bp.route('/debug/tasks', methods=['GET'])
@admin_required
def debug_tasks():
    """Debug endpoint to check task status"""
    try:
        from app.models import Task
        
        # Get all tasks
        all_tasks = Task.query.order_by(Task.created_at.desc()).limit(20).all()
        
        tasks_data = []
        for task in all_tasks:
            tasks_data.append({
                'id': task.id,
                'name': task.name,
                'task_type': task.task_type,
                'status': task.status,
                'created_at': task.created_at.isoformat(),
                'created_by': task.created_by.username if task.created_by else None,
                'permission_request_id': task.permission_request_id,
                'attempt_count': task.attempt_count,
                'max_attempts': task.max_attempts,
                'next_execution_at': task.next_execution_at.isoformat() if task.next_execution_at else None,
                'error_message': task.error_message
            })
        
        # Get task counts by status
        from sqlalchemy import func
        status_counts = db.session.query(
            Task.status, 
            func.count(Task.id)
        ).group_by(Task.status).all()
        
        return jsonify({
            'success': True,
            'tasks': tasks_data,
            'total_tasks': len(tasks_data),
            'status_counts': dict(status_counts)
        })
        
    except Exception as e:
        current_app.logger.error(f"Error in debug tasks endpoint: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@admin_bp.route('/debug/test-task-creation', methods=['POST'])
@admin_required
def test_task_creation():
    """Test endpoint to manually create tasks for a permission request"""
    try:
        data = request.get_json()
        request_id = data.get('request_id')
        
        if not request_id:
            return jsonify({
                'success': False,
                'error': 'Request ID required'
            }), 400
        
        from app.models import PermissionRequest, Task
        from app.services.task_service import TaskService
        from app.services.airflow_service import AirflowService
        
        permission_request = PermissionRequest.query.get_or_404(request_id)
        
        # Create tasks
        task_service = TaskService()
        airflow_service = AirflowService()
        
        current_app.logger.info(f"Testing task creation for request {request_id}")
        
        # Generate CSV file
        csv_file_path = airflow_service.create_permission_change_file([permission_request])
        current_app.logger.info(f"CSV file path: {csv_file_path}")
        
        # Create tasks
        tasks = task_service.create_approval_tasks(permission_request, current_user, csv_file_path)
        
        if tasks:
            tasks_info = []
            for task in tasks:
                tasks_info.append({
                    'id': task.id,
                    'name': task.name,
                    'task_type': task.task_type,
                    'status': task.status,
                    'created_by_id': task.created_by_id
                })
            
            return jsonify({
                'success': True,
                'message': f'Created {len(tasks)} tasks successfully',
                'tasks': tasks_info,
                'csv_file_path': csv_file_path
            })
        else:
            return jsonify({
                'success': False,
                'error': 'No tasks were created'
            })
        
    except Exception as e:
        current_app.logger.error(f"Error testing task creation: {str(e)}")
        import traceback
        current_app.logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@admin_bp.route('/backup')
@login_required
@admin_required
def backup_page():
    """Show backup management page"""
    # Get basic stats for the backup page
    stats = {
        'total_users': User.query.filter_by(is_active=True).count(),
        'total_folders': Folder.query.filter_by(is_active=True).count(),
        'total_ad_groups': ADGroup.query.filter_by(is_active=True).count(),
        'total_permissions': FolderPermission.query.filter_by(is_active=True).count()
    }
    return render_template('admin/backup.html', title='Backup del Sistema', stats=stats)


@admin_bp.route('/create-backup', methods=['POST'])
@login_required
@admin_required
def create_backup():
    """Create a minimal, stable backup of the system"""
    try:
        # Log audit event for backup start
        AuditEvent.log_event(
            user=current_user,
            event_type='system_backup',
            action='backup_started',
            resource_type='system',
            resource_id=None,
            description='Inicio de creación de backup del sistema',
            metadata={'initiated_by': current_user.username},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        current_app.logger.info(f"Backup initiated by user: {current_user.username}")
        
        # Create temporary directory
        temp_dir = tempfile.mkdtemp(prefix='sar_backup_')
        current_app.logger.info(f"Created temporary backup directory: {temp_dir}")
        
        backup_info = {
            'backup_date': datetime.now().isoformat(),
            'created_by': current_user.username,
            'files_included': [],
            'database_tables': [],
            'success': True
        }
        
        # 1. Export essential database tables to JSON
        current_app.logger.info("Starting minimal database export...")
        database_dir = os.path.join(temp_dir, 'database')
        os.makedirs(database_dir, exist_ok=True)
        
        # Define all models for complete backup (models are already imported at top of file)
        try:
            # Import additional models that may not be imported yet
            from app.models import AdminNotification, UserFolderPermission
            
            # Export ALL essential tables for complete backup
            models = [
                ('users', User),
                ('roles', Role), 
                ('folders', Folder),
                ('ad_groups', ADGroup),
                ('folder_permissions', FolderPermission),
                ('permission_requests', PermissionRequest),
                ('audit_events', AuditEvent),
                ('tasks', Task),
                ('user_ad_group_memberships', UserADGroupMembership),
                ('admin_notifications', AdminNotification),
                ('user_folder_permissions', UserFolderPermission)
            ]
        except ImportError as import_error:
            current_app.logger.error(f"Error importing additional models: {str(import_error)}")
            # Fallback to basic models if additional imports fail
            models = [
                ('users', User),
                ('roles', Role), 
                ('folders', Folder),
                ('ad_groups', ADGroup),
                ('folder_permissions', FolderPermission),
                ('permission_requests', PermissionRequest),
                ('audit_events', AuditEvent),
                ('tasks', Task),
                ('user_ad_group_memberships', UserADGroupMembership)
            ]
        
        total_records = 0
        for table_name, model_class in models:
            try:
                current_app.logger.info(f"Exporting table: {table_name}")
                records = model_class.query.all()
                table_data = []
                
                for record in records:
                    record_dict = {}
                    try:
                        for column in model_class.__table__.columns:
                            value = getattr(record, column.name, None)
                            if hasattr(value, 'isoformat'):
                                record_dict[column.name] = value.isoformat()
                            else:
                                record_dict[column.name] = str(value) if value is not None else None
                        table_data.append(record_dict)
                    except Exception as record_error:
                        current_app.logger.warning(f"Error processing record in {table_name}: {str(record_error)}")
                        continue
                
                # Save to JSON
                table_file = os.path.join(database_dir, f'{table_name}.json')
                with open(table_file, 'w', encoding='utf-8') as f:
                    json.dump(table_data, f, indent=2, ensure_ascii=False, default=str)
                
                backup_info['database_tables'].append({
                    'table': table_name,
                    'records': len(table_data),
                    'file': f'database/{table_name}.json'
                })
                
                total_records += len(table_data)
                current_app.logger.info(f"Exported {len(table_data)} records from {table_name}")
                
            except Exception as table_error:
                current_app.logger.error(f"Error exporting table {table_name}: {str(table_error)}")
                backup_info['database_tables'].append({
                    'table': table_name,
                    'records': 0,
                    'error': str(table_error)[:200]  # Limit error length
                })
        
        # Export many-to-many relationship tables that don't have explicit models
        current_app.logger.info("Exporting many-to-many relationship tables...")
        relationship_tables = [
            'user_roles',
            'folder_owners', 
            'folder_validators'
        ]
        
        for table_name in relationship_tables:
            try:
                current_app.logger.info(f"Exporting relationship table: {table_name}")
                
                # Query the table directly using raw SQL
                result = db.session.execute(text(f"SELECT * FROM {table_name}"))
                rows = result.fetchall()
                
                # Convert rows to list of dictionaries
                table_data = []
                if rows:
                    # Get column names from the first row
                    columns = list(rows[0]._mapping.keys())
                    for row in rows:
                        row_dict = {col: row._mapping[col] for col in columns}
                        table_data.append(row_dict)
                
                # Save to JSON
                table_file = os.path.join(database_dir, f'{table_name}.json')
                with open(table_file, 'w', encoding='utf-8') as f:
                    json.dump(table_data, f, indent=2, ensure_ascii=False, default=str)
                
                backup_info['database_tables'].append({
                    'table': table_name,
                    'records': len(table_data),
                    'file': f'database/{table_name}.json'
                })
                
                total_records += len(table_data)
                current_app.logger.info(f"Exported {len(table_data)} records from relationship table {table_name}")
                
            except Exception as table_error:
                current_app.logger.error(f"Error exporting relationship table {table_name}: {str(table_error)}")
                backup_info['database_tables'].append({
                    'table': table_name,
                    'records': 0,
                    'error': str(table_error)[:200]
                })
        
        # 2. Copy .env file (if exists)
        current_app.logger.info("Looking for .env configuration file...")
        try:
            env_locations = ['.env', '/app/.env', os.path.join(os.getcwd(), '.env')]
            env_copied = False
            
            for env_source in env_locations:
                if os.path.exists(env_source):
                    env_dest = os.path.join(temp_dir, '.env')
                    shutil.copy2(env_source, env_dest)
                    backup_info['files_included'].append('.env')
                    current_app.logger.info(f"Environment file copied from: {env_source}")
                    env_copied = True
                    break
            
            if not env_copied:
                current_app.logger.warning("No .env file found in expected locations")
                
        except Exception as env_error:
            current_app.logger.error(f"Error copying .env file: {str(env_error)}")
        
        # 3. Create backup metadata
        current_app.logger.info("Creating backup metadata...")
        try:
            metadata_file = os.path.join(temp_dir, 'backup_metadata.json')
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(backup_info, f, indent=2, ensure_ascii=False, default=str)
            backup_info['files_included'].append('backup_metadata.json')
        except Exception as meta_error:
            current_app.logger.error(f"Error creating metadata: {str(meta_error)}")
        
        # 4. Create simple README
        current_app.logger.info("Creating README...")
        try:
            readme_file = os.path.join(temp_dir, 'README.md')
            readme_content = f"""# SAR System Backup

**Backup Date**: {backup_info['backup_date']}
**Created By**: {backup_info['created_by']}
**Total Tables**: {len(backup_info['database_tables'])}
**Total Records**: {total_records}

## Contents
- `database/` - Database tables in JSON format
- `.env` - Environment configuration file (if available)
- `backup_metadata.json` - Backup information

## Restoration Steps
1. Set up PostgreSQL database
2. Copy .env to application directory
3. Initialize schema: `flask db init && flask db migrate && flask db upgrade`
4. Import JSON data using application tools
5. Restart application

For support, contact system administrator.
"""
            with open(readme_file, 'w', encoding='utf-8') as f:
                f.write(readme_content)
            backup_info['files_included'].append('README.md')
        except Exception as readme_error:
            current_app.logger.error(f"Error creating README: {str(readme_error)}")
        
        # 5. Create ZIP file
        current_app.logger.info("Creating backup ZIP file...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        zip_filename = f'sar_backup_{timestamp}.zip'
        # Ensure backup directory exists
        os.makedirs(BACKUP_DIRECTORY, exist_ok=True)
        zip_path = os.path.join(BACKUP_DIRECTORY, zip_filename)
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arc_name = os.path.relpath(file_path, temp_dir)
                    zipf.write(file_path, arc_name)
        
        # Ensure ZIP file is completely written to disk
        import time
        time.sleep(0.1)  # Small delay to ensure file is flushed to disk
        
        # Verify ZIP file integrity and size
        if not os.path.exists(zip_path):
            raise Exception(f"ZIP file was not created: {zip_path}")
        
        zip_size = os.path.getsize(zip_path)
        if zip_size == 0:
            raise Exception(f"ZIP file is empty: {zip_path}")
        
        current_app.logger.info(f"ZIP file created successfully: {zip_path} ({zip_size} bytes)")
        
        # Clean up temp directory
        shutil.rmtree(temp_dir)
        
        # Log successful backup
        AuditEvent.log_event(
            user=current_user,
            event_type='system_backup',
            action='backup_completed',
            resource_type='system',
            resource_id=None,
            description='Backup completo del sistema creado exitosamente',
            metadata={
                'backup_file': zip_filename,
                'database_tables': len([t for t in backup_info['database_tables'] if 'records' in t]),
                'total_records': total_records,
                'files_included': backup_info['files_included']
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        current_app.logger.info(f"Backup completed successfully: {zip_filename}")
        
        # Return success message instead of downloading file
        flash('Backup creado exitosamente. Puedes descargarlo desde la lista de backups.', 'success')
        return redirect(url_for('admin.backup_page'))
        
    except Exception as e:
        current_app.logger.error(f"Critical error creating backup: {str(e)}")
        
        # Clean up temp directory on error
        try:
            if 'temp_dir' in locals():
                shutil.rmtree(temp_dir)
        except:
            pass
        
        # Log failed backup
        try:
            AuditEvent.log_event(
                user=current_user,
                event_type='system_backup',
                action='backup_failed',
                resource_type='system',
                resource_id=None,
                description=f'Error creando backup del sistema: {str(e)}',
                metadata={'error': str(e)[:500]},  # Limit error message length
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        except Exception as audit_error:
            current_app.logger.error(f"Could not log backup failure to audit: {str(audit_error)}")
        
        flash(f'Error creando el backup: {str(e)}', 'error')
        return redirect(url_for('admin.backup_page'))


@admin_bp.route('/list-backups')
@login_required
@admin_required
def list_backups():
    """List all available backup files"""
    try:
        import glob
        import os
        from datetime import datetime
        
        # Get all backup files from temp directory
        backup_pattern = os.path.join(BACKUP_DIRECTORY, 'sar_backup_*.zip')
        backup_files = glob.glob(backup_pattern)
        
        backups = []
        for backup_file in sorted(backup_files, reverse=True):
            try:
                filename = os.path.basename(backup_file)
                size = os.path.getsize(backup_file)
                
                # Extract timestamp from filename
                timestamp_str = filename.replace('sar_backup_', '').replace('.zip', '')
                try:
                    created_date = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
                    formatted_date = created_date.strftime('%d/%m/%Y %H:%M:%S')
                except ValueError:
                    formatted_date = 'Fecha desconocida'
                
                backups.append({
                    'filename': filename,
                    'full_path': backup_file,
                    'size': size,
                    'size_mb': round(size / 1024 / 1024, 2) if size > 1024*1024 else round(size / 1024, 2),
                    'size_unit': 'MB' if size > 1024*1024 else 'KB',
                    'created_date': formatted_date,
                    'timestamp': timestamp_str
                })
            except Exception as file_error:
                current_app.logger.error(f"Error processing backup file {backup_file}: {str(file_error)}")
                continue
        
        return jsonify({
            'success': True,
            'backups': backups,
            'count': len(backups)
        })
        
    except Exception as e:
        current_app.logger.error(f"Error listing backups: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@admin_bp.route('/download-backup/<filename>')
@login_required
@admin_required
def download_backup(filename):
    """Download a specific backup file"""
    try:
        import os
        import re
        
        # Validate filename format for security
        if not re.match(r'^sar_backup_.*\d{8}_\d{6}\.zip$', filename):
            flash('Nombre de archivo de backup inválido', 'error')
            return redirect(url_for('admin.backup_page'))
        
        backup_path = os.path.join(BACKUP_DIRECTORY, filename)
        
        if not os.path.exists(backup_path):
            flash('Archivo de backup no encontrado', 'error')
            return redirect(url_for('admin.backup_page'))
        
        # Log download event
        AuditEvent.log_event(
            user=current_user,
            event_type='system_backup',
            action='backup_downloaded',
            resource_type='system',
            resource_id=None,
            description=f'Backup descargado: {filename}',
            metadata={'backup_file': filename},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        current_app.logger.info(f"Backup downloaded by user {current_user.username}: {filename}")
        
        return send_file(
            backup_path,
            as_attachment=True,
            download_name=filename,
            mimetype='application/zip'
        )
        
    except Exception as e:
        current_app.logger.error(f"Error downloading backup {filename}: {str(e)}")
        flash(f'Error descargando el backup: {str(e)}', 'error')
        return redirect(url_for('admin.backup_page'))


@admin_bp.route('/delete-backup/<filename>', methods=['POST'])
@login_required
@admin_required
def delete_backup(filename):
    """Delete a specific backup file"""
    try:
        import os
        import re
        
        # Validate filename format for security
        if not re.match(r'^sar_backup_.*\d{8}_\d{6}\.zip$', filename):
            return jsonify({
                'success': False,
                'error': 'Nombre de archivo de backup inválido'
            }), 400
        
        backup_path = os.path.join(BACKUP_DIRECTORY, filename)
        
        if not os.path.exists(backup_path):
            return jsonify({
                'success': False,
                'error': 'Archivo de backup no encontrado'
            }), 404
        
        # Delete the file
        os.remove(backup_path)
        
        # Log deletion event
        AuditEvent.log_event(
            user=current_user,
            event_type='system_backup',
            action='backup_deleted',
            resource_type='system',
            resource_id=None,
            description=f'Backup eliminado: {filename}',
            metadata={'backup_file': filename},
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        current_app.logger.info(f"Backup deleted by user {current_user.username}: {filename}")
        
        return jsonify({
            'success': True,
            'message': f'Backup {filename} eliminado exitosamente'
        })
        
    except Exception as e:
        current_app.logger.error(f"Error deleting backup {filename}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@admin_bp.route('/restore-backup/<filename>', methods=['POST'])
@login_required
@admin_required
def restore_backup(filename):
    """Restore system from a specific backup file"""
    try:
        import os
        import re
        import json
        import zipfile
        from sqlalchemy import text
        
        # Validate filename format for security
        if not re.match(r'^sar_backup_.*\d{8}_\d{6}\.zip$', filename):
            return jsonify({
                'success': False,
                'error': 'Nombre de archivo de backup inválido'
            }), 400
        
        backup_path = os.path.join(BACKUP_DIRECTORY, filename)
        
        if not os.path.exists(backup_path):
            return jsonify({
                'success': False,
                'error': 'Archivo de backup no encontrado'
            }), 404
        
        current_app.logger.info(f"Starting restore process from backup: {filename}")
        
        # Create temporary directory to extract backup
        extract_dir = tempfile.mkdtemp(prefix='sar_restore_')
        
        try:
            # Extract ZIP file
            with zipfile.ZipFile(backup_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Validate backup structure
            required_files = ['backup_metadata.json']
            database_dir = os.path.join(extract_dir, 'database')
            
            if not os.path.exists(database_dir):
                return jsonify({
                    'success': False,
                    'error': 'Estructura de backup inválida: falta directorio database'
                }), 400
            
            # Load backup metadata
            metadata_file = os.path.join(extract_dir, 'backup_metadata.json')
            if not os.path.exists(metadata_file):
                return jsonify({
                    'success': False,
                    'error': 'Estructura de backup inválida: falta metadata'
                }), 400
            
            with open(metadata_file, 'r', encoding='utf-8') as f:
                backup_metadata = json.load(f)
            
            current_app.logger.info(f"Backup metadata loaded: {backup_metadata.get('backup_date', 'Unknown date')}")
            
            # WARNING: This is a destructive operation
            # In production, you might want to create a backup before restore
            current_app.logger.warning("DESTRUCTIVE OPERATION: Starting database restoration")
            
            # Get list of JSON files to restore
            json_files = []
            for file in os.listdir(database_dir):
                if file.endswith('.json'):
                    table_name = file.replace('.json', '')
                    json_path = os.path.join(database_dir, file)
                    json_files.append((table_name, json_path))
            
            if not json_files:
                return jsonify({
                    'success': False,
                    'error': 'No se encontraron datos de tablas para restaurar'
                }), 400
            
            restored_tables = []
            restored_records = 0
            
            # Disable foreign key checks temporarily
            db.session.execute(text("SET session_replication_role = replica;"))
            
            try:
                # Restore each table
                for table_name, json_path in json_files:
                    try:
                        current_app.logger.info(f"Restoring table: {table_name}")
                        
                        # Load JSON data
                        with open(json_path, 'r', encoding='utf-8') as f:
                            table_data = json.load(f)
                        
                        if not table_data:
                            current_app.logger.warning(f"Table {table_name} has no data to restore")
                            continue
                        
                        # Clear existing data from table
                        result = db.session.execute(text(f"DELETE FROM {table_name}"))
                        deleted_count = result.rowcount
                        current_app.logger.info(f"Cleared {deleted_count} existing records from {table_name}")
                        
                        # Insert restored data
                        inserted_count = 0
                        for record in table_data:
                            try:
                                # Build INSERT statement dynamically
                                columns = list(record.keys())
                                placeholders = [f":{col}" for col in columns]
                                
                                insert_sql = f"""
                                    INSERT INTO {table_name} ({', '.join(columns)})
                                    VALUES ({', '.join(placeholders)})
                                """
                                
                                db.session.execute(text(insert_sql), record)
                                inserted_count += 1
                                
                            except Exception as record_error:
                                current_app.logger.error(f"Error inserting record in {table_name}: {str(record_error)}")
                                continue
                        
                        restored_tables.append({
                            'table': table_name,
                            'records_deleted': deleted_count,
                            'records_inserted': inserted_count
                        })
                        
                        restored_records += inserted_count
                        current_app.logger.info(f"Restored {inserted_count} records to {table_name}")
                        
                    except Exception as table_error:
                        current_app.logger.error(f"Error restoring table {table_name}: {str(table_error)}")
                        restored_tables.append({
                            'table': table_name,
                            'error': str(table_error)[:200]
                        })
                        continue
                
                # Commit all changes
                db.session.commit()
                current_app.logger.info("Database restoration completed successfully")
                
            finally:
                # Re-enable foreign key checks
                db.session.execute(text("SET session_replication_role = DEFAULT;"))
                db.session.commit()
            
            # Log successful restoration
            AuditEvent.log_event(
                user=current_user,
                event_type='system_restore',
                action='backup_restored',
                resource_type='system',
                resource_id=None,
                description=f'Sistema restaurado desde backup: {filename}',
                metadata={
                    'backup_file': filename,
                    'restored_tables': len([t for t in restored_tables if 'error' not in t]),
                    'total_records': restored_records,
                    'backup_date': backup_metadata.get('backup_date'),
                    'tables_restored': restored_tables
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            current_app.logger.info(f"Restore completed by user {current_user.username}: {filename}")
            
            return jsonify({
                'success': True,
                'message': f'Sistema restaurado exitosamente desde {filename}',
                'details': {
                    'backup_date': backup_metadata.get('backup_date'),
                    'restored_tables': len([t for t in restored_tables if 'error' not in t]),
                    'total_records': restored_records,
                    'tables_info': restored_tables
                }
            })
            
        finally:
            # Clean up temporary extraction directory
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir)
        
    except Exception as e:
        current_app.logger.error(f"Critical error restoring backup {filename}: {str(e)}")
        
        # Rollback any partial changes
        try:
            db.session.rollback()
        except:
            pass
        
        # Log failed restoration
        try:
            AuditEvent.log_event(
                user=current_user,
                event_type='system_restore',
                action='restore_failed',
                resource_type='system',
                resource_id=None,
                description=f'Error restaurando backup: {filename} - {str(e)}',
                metadata={'backup_file': filename, 'error': str(e)[:500]},
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
        except Exception as audit_error:
            current_app.logger.error(f"Could not log restore failure to audit: {str(audit_error)}")
        
        return jsonify({
            'success': False,
            'error': f'Error crítico durante la restauración: {str(e)}'
        }), 500