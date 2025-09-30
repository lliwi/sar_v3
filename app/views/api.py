from flask import Blueprint, request, jsonify, current_app, send_file, abort, render_template
from flask_login import login_required, current_user
from app.models import PermissionRequest, AuditEvent, Folder, FolderPermission, ADGroup, User
from app import db
from functools import wraps
from datetime import datetime
import secrets
import hashlib
import os

api_bp = Blueprint('api', __name__)

def generate_validation_token(request_id):
    """Generate secure validation token for email links"""
    secret = current_app.config['SECRET_KEY']
    data = f"{request_id}:{secret}"
    return hashlib.sha256(data.encode()).hexdigest()

def verify_validation_token(request_id, token):
    """Verify validation token"""
    expected_token = generate_validation_token(request_id)
    return secrets.compare_digest(expected_token, token)

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin():
            return jsonify({'error': 'Acceso denegado. Se requieren permisos de administrador.'}), 403
        return f(*args, **kwargs)
    return decorated_function

@api_bp.route('/users/active')
@login_required
def get_active_users():
    """Get active users for combobox selection"""
    try:
        users = User.query.filter_by(is_active=True).order_by(User.full_name).all()
        
        user_list = []
        for user in users:
            user_list.append({
                'id': user.id,
                'name': user.full_name,
                'secondary': f"{user.username} - {user.department or 'Sin departamento'}"
            })
        
        return jsonify({
            'success': True,
            'items': user_list
        })
    except Exception as e:
        current_app.logger.error(f"Error loading users: {e}")
        return jsonify({
            'success': False,
            'error': 'Error al cargar usuarios'
        }), 500

@api_bp.route('/ad-groups/active')
@login_required  
def get_active_ad_groups():
    """Get active AD groups for combobox selection"""
    try:
        groups = ADGroup.query.filter_by(is_active=True).order_by(ADGroup.name).all()
        
        group_list = []
        for group in groups:
            group_list.append({
                'id': group.id,
                'name': group.name,
                'secondary': group.description or 'Sin descripción'
            })
        
        return jsonify({
            'success': True,
            'items': group_list
        })
    except Exception as e:
        current_app.logger.error(f"Error loading AD groups: {e}")
        return jsonify({
            'success': False,
            'error': 'Error al cargar grupos de AD'
        }), 500

@api_bp.route('/validate-permission/<int:request_id>/<token>')
def validate_permission_by_email(request_id, token):
    """API endpoint for email validation links"""
    if not verify_validation_token(request_id, token):
        return render_template('api/validation_result.html',
                             success=False,
                             title='Error de Validación',
                             message='Token de validación inválido o expirado'), 400

    permission_request = PermissionRequest.query.get_or_404(request_id)

    if not permission_request.is_pending():
        return render_template('api/validation_result.html',
                             success=False,
                             title='Solicitud Ya Procesada',
                             message='Esta solicitud ya ha sido procesada anteriormente'), 400
    
    action = request.args.get('action')  # 'approve' or 'reject'
    comment = request.args.get('comment', '')

    if action not in ['approve', 'reject']:
        return render_template('api/validation_result.html',
                             success=False,
                             title='Error',
                             message='Acción inválida'), 400

    # For security, we'll require the validator to be one of the folder owners/validators
    folder = permission_request.folder
    if not any(owner.email for owner in folder.owners) and not any(validator.email for validator in folder.validators):
        return render_template('api/validation_result.html',
                             success=False,
                             title='Error',
                             message='No se encontraron validadores autorizados'), 400

    # We'll use the first owner/validator as the validator for the API call
    validator = None
    if folder.owners:
        validator = folder.owners[0]
    elif folder.validators:
        validator = folder.validators[0]

    if not validator:
        return render_template('api/validation_result.html',
                             success=False,
                             title='Error',
                             message='No se encontró un validador para esta carpeta'), 400
    
    if action == 'approve':
        # Check if this is a change request (has special metadata)
        if hasattr(permission_request, '_is_change_request') and permission_request._is_change_request:
            permission_request.approve_with_change(validator, comment)
        else:
            permission_request.approve(validator, comment)
        
        # Check if folder permission already exists
        existing_permission = FolderPermission.query.filter_by(
            folder_id=permission_request.folder_id,
            ad_group_id=permission_request.ad_group_id,
            permission_type=permission_request.permission_type
        ).first()
        
        if not existing_permission:
            # Create the folder permission
            folder_permission = FolderPermission(
                folder_id=permission_request.folder_id,
                ad_group_id=permission_request.ad_group_id,
                permission_type=permission_request.permission_type,
                granted_by=validator
            )
            db.session.add(folder_permission)
        else:
            # Update existing permission if it was inactive
            if not existing_permission.is_active:
                existing_permission.is_active = True
                existing_permission.granted_by = validator
                existing_permission.granted_at = datetime.utcnow()
            current_app.logger.info(f'Permission already exists for request {permission_request.id}, skipping creation')
        
        # Note: Tasks are automatically created by the permission_request.approve() method above
        # No need to create tasks manually here to avoid duplication
        current_app.logger.info(f"Email approval completed for permission request {permission_request.id}. Tasks created automatically by approve() method.")
        
        # Log audit event
        AuditEvent.log_event(
            user=validator,
            event_type='permission_request',
            action='approve_email',
            resource_type='permission_request',
            resource_id=permission_request.id,
            description=f'Solicitud aprobada vía email para {permission_request.folder.path}',
            metadata={
                'folder_path': permission_request.folder.path,
                'ad_group': permission_request.ad_group.name,
                'permission_type': permission_request.permission_type,
                'comment': comment,
                'validation_method': 'email'
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        message = 'Solicitud aprobada exitosamente'
    
    elif action == 'reject':
        permission_request.reject(validator, comment)
        
        # Log audit event
        AuditEvent.log_event(
            user=validator,
            event_type='permission_request',
            action='reject_email',
            resource_type='permission_request',
            resource_id=permission_request.id,
            description=f'Solicitud rechazada vía email para {permission_request.folder.path}',
            metadata={
                'folder_path': permission_request.folder.path,
                'ad_group': permission_request.ad_group.name,
                'permission_type': permission_request.permission_type,
                'comment': comment,
                'validation_method': 'email'
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        message = 'Solicitud rechazada exitosamente'
    
    db.session.commit()

    return render_template('api/validation_result.html',
                         success=True,
                         title='Solicitud Procesada',
                         message=message)

@api_bp.route('/audit-events')
@login_required
@admin_required
def get_audit_events():
    """Get audit events with filters"""
    page = request.args.get('page', 1, type=int)
    per_page = min(request.args.get('per_page', 50, type=int), 100)
    
    # Filters
    user_id = request.args.get('user_id', type=int)
    event_type = request.args.get('event_type')
    action = request.args.get('action')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    query = AuditEvent.query
    
    if user_id:
        query = query.filter_by(user_id=user_id)
    if event_type:
        query = query.filter_by(event_type=event_type)
    if action:
        query = query.filter_by(action=action)
    if start_date:
        query = query.filter(AuditEvent.created_at >= start_date)
    if end_date:
        query = query.filter(AuditEvent.created_at <= end_date)
    
    events = query.order_by(AuditEvent.created_at.desc()).paginate(
        page=page, per_page=per_page, error_out=False
    )
    
    return jsonify({
        'events': [event.to_dict() for event in events.items],
        'pagination': {
            'page': events.page,
            'pages': events.pages,
            'per_page': events.per_page,
            'total': events.total,
            'has_next': events.has_next,
            'has_prev': events.has_prev
        }
    })

@api_bp.route('/permissions-report')
@login_required
@admin_required
def permissions_report():
    """Get comprehensive permissions report"""
    folder_id = request.args.get('folder_id', type=int)
    ad_group_id = request.args.get('ad_group_id', type=int)
    
    query = FolderPermission.query.filter_by(is_active=True)
    
    if folder_id:
        query = query.filter_by(folder_id=folder_id)
    if ad_group_id:
        query = query.filter_by(ad_group_id=ad_group_id)
    
    permissions = query.all()
    
    report = []
    for perm in permissions:
        report.append({
            'folder_path': perm.folder.path,
            'folder_name': perm.folder.name,
            'ad_group_name': perm.ad_group.name,
            'permission_type': perm.permission_type,
            'granted_by': perm.granted_by.username if perm.granted_by else None,
            'granted_at': perm.granted_at.isoformat(),
            'folder_owners': [owner.username for owner in perm.folder.owners],
            'folder_validators': [validator.username for validator in perm.folder.validators]
        })
    
    return jsonify({
        'permissions': report,
        'total_count': len(report),
        'generated_at': db.func.now().isoformat()
    })

@api_bp.route('/folders')
@login_required
def get_folders():
    """Get list of folders (for dropdown/autocomplete)"""
    search = request.args.get('search', '')
    active_only = request.args.get('active_only', 'true').lower() == 'true'
    
    query = Folder.query
    
    if active_only:
        query = query.filter_by(is_active=True)
    
    if search:
        query = query.filter(
            db.or_(
                Folder.name.ilike(f'%{search}%'),
                Folder.path.ilike(f'%{search}%')
            )
        )
    
    folders = query.order_by(Folder.path).limit(50).all()
    
    return jsonify({
        'folders': [folder.to_dict() for folder in folders]
    })

@api_bp.route('/ad-groups')
@login_required
def get_ad_groups():
    """Get list of AD groups (for dropdown/autocomplete)"""
    search = request.args.get('search', '')
    active_only = request.args.get('active_only', 'true').lower() == 'true'
    
    query = ADGroup.query
    
    if active_only:
        query = query.filter_by(is_active=True)
    
    if search:
        query = query.filter(
            db.or_(
                ADGroup.name.ilike(f'%{search}%'),
                ADGroup.description.ilike(f'%{search}%')
            )
        )
    
    groups = query.order_by(ADGroup.name).limit(50).all()
    
    return jsonify({
        'ad_groups': [group.to_dict() for group in groups]
    })

@api_bp.route('/users')
@login_required
@admin_required
def get_users():
    """Get list of users"""
    search = request.args.get('search', '')
    active_only = request.args.get('active_only', 'true').lower() == 'true'
    
    query = User.query
    
    if active_only:
        query = query.filter_by(is_active=True)
    
    if search:
        query = query.filter(
            db.or_(
                User.username.ilike(f'%{search}%'),
                User.full_name.ilike(f'%{search}%'),
                User.email.ilike(f'%{search}%')
            )
        )
    
    users = query.order_by(User.full_name).limit(50).all()
    
    return jsonify({
        'users': [user.to_dict() for user in users]
    })

@api_bp.route('/cancel-request/<int:request_id>', methods=['POST'])
@login_required
def cancel_request(request_id):
    """Cancel a pending permission request"""
    try:
        permission_request = PermissionRequest.query.get_or_404(request_id)
        
        # Only the requester or an admin can cancel a request
        if permission_request.requester_id != current_user.id and not current_user.is_admin():
            return jsonify({
                'success': False,
                'message': 'No tienes permisos para cancelar esta solicitud'
            }), 403
        
        # Can only cancel pending requests
        if permission_request.status != 'pending':
            return jsonify({
                'success': False,
                'message': 'Solo se pueden cancelar solicitudes pendientes'
            }), 400
        
        # Cancel the request using the model method
        permission_request.cancel(current_user, 'Solicitud cancelada por el usuario')
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='permission_request',
            action='cancel',
            resource_type='permission_request',
            resource_id=permission_request.id,
            description=f'Solicitud cancelada para {permission_request.folder.path}',
            metadata={
                'folder_path': permission_request.folder.path,
                'ad_group': permission_request.ad_group.name,
                'permission_type': permission_request.permission_type,
                'canceled_by': current_user.username
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Solicitud cancelada exitosamente'
        })
    
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Error al cancelar la solicitud: {str(e)}'
        }), 500

@api_bp.route('/validate-request/<int:request_id>', methods=['POST'])
@login_required
def validate_request_api(request_id):
    """API endpoint for validating permission requests"""
    try:
        permission_request = PermissionRequest.query.get_or_404(request_id)
        
        # Check if request is still pending
        if not permission_request.is_pending():
            return jsonify({
                'success': False,
                'message': 'Esta solicitud ya ha sido procesada'
            }), 400
        
        # Check if user can validate this request
        if not permission_request.can_be_validated_by(current_user):
            return jsonify({
                'success': False,
                'message': 'No tienes permisos para validar esta solicitud'
            }), 403
        
        data = request.get_json()
        if not data or 'action' not in data:
            return jsonify({
                'success': False,
                'message': 'Acción requerida'
            }), 400
        
        action = data['action']
        comment = data.get('comment', '')
        
        if action not in ['approve', 'reject']:
            return jsonify({
                'success': False,
                'message': 'Acción inválida'
            }), 400
        
        if action == 'approve':
            # Check if this is a change request (has special metadata)
            if hasattr(permission_request, '_is_change_request') and permission_request._is_change_request:
                permission_request.approve_with_change(current_user, comment)
            else:
                permission_request.approve(current_user, comment)
            
            # Check if folder permission already exists
            from app.models.folder_permission import FolderPermission
            existing_permission = FolderPermission.query.filter_by(
                folder_id=permission_request.folder_id,
                ad_group_id=permission_request.ad_group_id,
                permission_type=permission_request.permission_type
            ).first()
            
            if not existing_permission:
                # Create the folder permission if approved
                folder_permission = FolderPermission(
                    folder_id=permission_request.folder_id,
                    ad_group_id=permission_request.ad_group_id,
                    permission_type=permission_request.permission_type,
                    granted_by=current_user
                )
                db.session.add(folder_permission)
            else:
                # Update existing permission if it was inactive
                if not existing_permission.is_active:
                    existing_permission.is_active = True
                    existing_permission.granted_by = current_user
                    existing_permission.granted_at = datetime.utcnow()
                current_app.logger.info(f'Permission already exists for request {permission_request.id}, skipping creation')
            
            # Note: Tasks are automatically created by the permission_request.approve() method above
            # No need to create tasks manually here to avoid duplication
            current_app.logger.info(f"Approval completed for permission request {permission_request.id}. Tasks created automatically by approve() method.")
            
            # AD validation is now handled by the verification task
            
            # Log audit event
            AuditEvent.log_event(
                user=current_user,
                event_type='permission_request',
                action='approve',
                resource_type='permission_request',
                resource_id=permission_request.id,
                description=f'Solicitud aprobada para {permission_request.folder.path}',
                metadata={
                    'folder_path': permission_request.folder.path,
                    'ad_group': permission_request.ad_group.name,
                    'permission_type': permission_request.permission_type,
                    'comment': comment
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            message = 'Solicitud aprobada exitosamente'
        
        elif action == 'reject':
            permission_request.reject(current_user, comment)
            
            # Log audit event
            AuditEvent.log_event(
                user=current_user,
                event_type='permission_request',
                action='reject',
                resource_type='permission_request',
                resource_id=permission_request.id,
                description=f'Solicitud rechazada para {permission_request.folder.path}',
                metadata={
                    'folder_path': permission_request.folder.path,
                    'ad_group': permission_request.ad_group.name if permission_request.ad_group else 'Sin grupo',
                    'permission_type': permission_request.permission_type,
                    'comment': comment
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            message = 'Solicitud rechazada exitosamente'
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'message': message,
            'request_id': request_id,
            'status': permission_request.status
        })
    
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'message': f'Error al procesar la solicitud: {str(e)}'
        }), 500


@api_bp.route('/test-airflow', methods=['POST'])
@login_required
@admin_required
def test_airflow():
    """Test Airflow DAG execution"""
    try:
        from app.services.airflow_service import AirflowService
        
        airflow_service = AirflowService()
        
        # Test configuration
        conf = {
            'test_message': 'Prueba de conectividad desde Flask',
            'triggered_by': current_user.username,
            'test_timestamp': datetime.utcnow().isoformat()
        }
        
        # Try to trigger the test DAG
        success = airflow_service.trigger_dag(conf)
        
        if success:
            # Log audit event
            AuditEvent.log_event(
                user=current_user,
                event_type='airflow_test',
                action='trigger_dag',
                description='Prueba de conectividad con Airflow',
                metadata={
                    'dag_id': airflow_service.dag_id,
                    'config': conf
                },
                ip_address=request.remote_addr,
                user_agent=request.headers.get('User-Agent')
            )
            
            return jsonify({
                'success': True,
                'message': f'DAG {airflow_service.dag_id} ejecutado exitosamente',
                'dag_id': airflow_service.dag_id,
                'config': conf
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Error al ejecutar DAG de Airflow'
            }), 500
    
    except ImportError:
        return jsonify({'error': 'Servicio de Airflow no disponible'}), 503
    except Exception as e:
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks')
@login_required
@admin_required
def get_tasks():
    """Get list of tasks with filtering options"""
    try:
        from app.models import Task
        
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 20, type=int), 100)
        
        # Filters
        task_type = request.args.get('task_type')
        status = request.args.get('status')
        permission_request_id = request.args.get('permission_request_id', type=int)
        
        query = Task.query
        
        if task_type:
            query = query.filter_by(task_type=task_type)
        if status:
            query = query.filter_by(status=status)
        if permission_request_id:
            query = query.filter_by(permission_request_id=permission_request_id)
        
        tasks = query.order_by(Task.created_at.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        return jsonify({
            'tasks': [task.to_dict() for task in tasks.items],
            'pagination': {
                'page': tasks.page,
                'pages': tasks.pages,
                'per_page': tasks.per_page,
                'total': tasks.total,
                'has_next': tasks.has_next,
                'has_prev': tasks.has_prev
            }
        })
    
    except Exception as e:
        current_app.logger.error(f"Error getting tasks: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks/<int:task_id>')
@login_required
@admin_required
def get_task(task_id):
    """Get specific task details"""
    try:
        from app.models import Task
        
        task = Task.query.get_or_404(task_id)
        return jsonify(task.to_dict())
    
    except Exception as e:
        current_app.logger.error(f"Error getting task {task_id}: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks/status/<int:permission_request_id>')
@login_required
def get_task_status(permission_request_id):
    """Get task status for a permission request"""
    try:
        from app.services.task_service import TaskService
        
        # Check if user can view this request
        permission_request = PermissionRequest.query.get_or_404(permission_request_id)
        
        # Users can view task status for their own requests or if they're admin/validator
        if (permission_request.requester_id != current_user.id and 
            not current_user.is_admin() and 
            not permission_request.can_be_validated_by(current_user)):
            return jsonify({'error': 'No tienes permisos para ver el estado de esta solicitud'}), 403
        
        task_service = TaskService()
        status = task_service.get_task_status(permission_request_id)
        
        return jsonify(status)
    
    except Exception as e:
        current_app.logger.error(f"Error getting task status for request {permission_request_id}: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks/retry/<int:task_id>', methods=['POST'])
@login_required
@admin_required
def retry_task(task_id):
    """Retry a failed task"""
    try:
        from app.models import Task
        
        task = Task.query.get_or_404(task_id)
        
        if not task.is_failed():
            return jsonify({
                'success': False,
                'message': 'Solo se pueden reintentar tareas fallidas'
            }), 400
        
        # Reset task for retry
        task.status = 'pending'
        task.attempt_count = 0
        task.next_execution_at = datetime.utcnow()
        task.error_message = None
        task.updated_at = datetime.utcnow()
        
        db.session.commit()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='task_management',
            action='retry_task',
            resource_type='task',
            resource_id=task.id,
            description=f'Tarea {task.name} marcada para reintento',
            metadata={
                'task_id': task.id,
                'task_type': task.task_type,
                'permission_request_id': task.permission_request_id
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': 'Tarea marcada para reintento exitosamente'
        })
    
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error retrying task {task_id}: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks/process', methods=['POST'])
@login_required
@admin_required
def process_pending_tasks():
    """Manually trigger processing of pending tasks"""
    try:
        from app.services.task_service import TaskService
        
        task_service = TaskService()
        processed_count = task_service.process_pending_tasks()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='task_management',
            action='process_tasks',
            description=f'Procesamiento manual de tareas pendientes ejecutado',
            metadata={
                'processed_count': processed_count,
                'triggered_by': current_user.username
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': f'Se procesaron {processed_count} tareas',
            'processed_count': processed_count
        })
    
    except Exception as e:
        current_app.logger.error(f"Error processing pending tasks: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks/mark-manual/<int:task_id>', methods=['POST'])
@login_required
@admin_required
def mark_task_manual(task_id):
    """Mark an Airflow task as manually completed"""
    try:
        from app.models import Task
        
        task = Task.query.get_or_404(task_id)
        
        # Only allow marking Airflow DAG tasks as manual
        if task.task_type != 'airflow_dag':
            return jsonify({
                'success': False,
                'message': f'Solo se pueden marcar tareas de Airflow como realizadas manualmente. Tipo actual: {task.task_type}'
            }), 400

        # Only allow marking pending, failed, retry, or running tasks
        if task.status not in ['pending', 'failed', 'retry', 'running']:
            return jsonify({
                'success': False,
                'message': f'Solo se pueden marcar como manuales tareas pendientes, fallidas, en reintento o en ejecución. Estado actual: {task.status}'
            }), 400
        
        # Mark task as completed with manual execution flag
        result_data = {
            'manual_completion': True,
            'manual_completion_time': datetime.utcnow().isoformat(),
            'manual_completion_user': current_user.username,
            'manual_completion_note': request.json.get('note', '') if request.is_json else '',
            'dag_execution_status': 'manual',
            'dag_id': task.get_task_data().get('dag_id', 'unknown'),
            'execution_time': datetime.utcnow().isoformat()
        }
        
        task.mark_as_completed(result_data)

        # Execute dependent tasks immediately when manually completing a task
        try:
            from app.services.task_service import TaskService
            task_service = TaskService()
            if hasattr(task_service, '_execute_dependent_tasks_immediately'):
                task_service._execute_dependent_tasks_immediately(task)
            else:
                # Fallback: manually activate dependent tasks
                dependent_tasks = Task.query.filter_by(status='pending').filter(
                    Task.task_data.contains(f'"depends_on_task_id": {task.id}')
                ).all()

                for dep_task in dependent_tasks:
                    dep_task.next_execution_at = datetime.utcnow()
                    current_app.logger.info(f"Manually scheduled dependent task {dep_task.id} after completing task {task.id}")

        except Exception as e:
            current_app.logger.error(f"Error executing dependent tasks: {str(e)}")
            # Don't fail the main operation if dependency scheduling fails

        db.session.commit()

        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='task_management',
            action='manual_completion',
            resource_type='task',
            resource_id=task.id,
            description=f'Tarea de Airflow {task.name} marcada como realizada manualmente',
            metadata={
                'task_id': task.id,
                'task_type': task.task_type,
                'permission_request_id': task.permission_request_id,
                'manual_note': request.json.get('note', '') if request.is_json else ''
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': 'Tarea marcada como realizada manualmente'
        })
    
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error marking task {task_id} as manual: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks/activate-dependencies/<int:task_id>', methods=['POST'])
@login_required
@admin_required
def activate_task_dependencies(task_id):
    """Activate dependent tasks for a completed task (useful for fixing dependency issues)"""
    try:
        task = Task.query.get_or_404(task_id)

        if not task.is_completed():
            return jsonify({
                'success': False,
                'message': 'Solo se pueden activar dependencias de tareas completadas'
            }), 400

        # Execute dependent tasks immediately
        try:
            from app.services.task_service import TaskService
            task_service = TaskService()
            if hasattr(task_service, '_execute_dependent_tasks_immediately'):
                task_service._execute_dependent_tasks_immediately(task)
            else:
                # Fallback: manually activate dependent tasks
                dependent_tasks = Task.query.filter_by(status='pending').filter(
                    Task.task_data.contains(f'"depends_on_task_id": {task.id}')
                ).all()

                for dep_task in dependent_tasks:
                    dep_task.next_execution_at = datetime.utcnow()
                    current_app.logger.info(f"Manually scheduled dependent task {dep_task.id} after completing task {task.id}")

        except Exception as e:
            current_app.logger.error(f"Error executing dependent tasks: {str(e)}")
            # Don't fail the main operation if dependency scheduling fails

        db.session.commit()

        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='task_management',
            action='activate_dependencies',
            resource_type='task',
            resource_id=task.id,
            description=f'Dependencias activadas manualmente para tarea {task.name}',
            metadata={
                'task_id': task.id,
                'task_type': task.task_type,
                'activation_user': current_user.username,
                'activation_time': datetime.utcnow().isoformat()
            }
        )

        return jsonify({
            'success': True,
            'message': f'Dependencias activadas para tarea {task.id}',
            'task_id': task.id
        }), 200

    except Exception as e:
        db.session.rollback()
        logger.error(f"Error activating dependencies for task {task_id}: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Error al activar dependencias: {str(e)}'
        }), 500

@api_bp.route('/tasks/cancel/<int:task_id>', methods=['POST'])
@login_required
@admin_required
def cancel_task(task_id):
    """Cancel a pending or retry task"""
    try:
        from app.models import Task
        
        task = Task.query.get_or_404(task_id)
        
        # Check if task can be cancelled
        if not task.can_be_cancelled():
            return jsonify({
                'success': False,
                'message': f'No se puede cancelar una tarea en estado "{task.status}"'
            }), 400
        
        # Get cancellation reason from request
        reason = None
        if request.is_json and request.json:
            reason = request.json.get('reason', 'Cancelado por el administrador')
        else:
            reason = 'Cancelado por el administrador'
        
        # Cancel the task
        task.cancel(cancelled_by=current_user, reason=reason)

        # Clean up CSV file if it's an AD verification task
        if task.task_type == 'ad_verification':
            try:
                from app.services.task_service import TaskService
                task_service = TaskService()
                task_service.cleanup_csv_file(task)
            except Exception as e:
                current_app.logger.warning(f"Error cleaning up CSV file for cancelled task {task.id}: {str(e)}")

        db.session.commit()
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='task_management',
            action='cancel_task',
            resource_type='task',
            resource_id=task.id,
            description=f'Tarea {task.name} cancelada',
            metadata={
                'task_id': task.id,
                'task_type': task.task_type,
                'permission_request_id': task.permission_request_id,
                'cancellation_reason': reason,
                'previous_status': 'pending' if task.attempt_count == 0 else 'retry'
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': 'Tarea cancelada exitosamente'
        })
    
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error cancelling task {task_id}: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks/stats')
@login_required  
@admin_required
def get_task_stats():
    """Get updated task statistics"""
    try:
        from app.models import Task
        
        stats = {
            'total_tasks': Task.query.count(),
            'pending_tasks': Task.query.filter_by(status='pending').count(),
            'running_tasks': Task.query.filter_by(status='running').count(),
            'completed_tasks': Task.query.filter_by(status='completed').count(),
            'failed_tasks': Task.query.filter_by(status='failed').count(),
            'retry_tasks': Task.query.filter_by(status='retry').count(),
            'cancelled_tasks': Task.query.filter_by(status='cancelled').count()
        }
        
        return jsonify({
            'success': True,
            'stats': stats
        })
    
    except Exception as e:
        current_app.logger.error(f"Error getting task stats: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/tasks/cleanup', methods=['POST'])
@login_required
@admin_required
def cleanup_old_tasks():
    """Clean up old completed/failed tasks"""
    try:
        from app.services.task_service import TaskService
        
        days_old = request.json.get('days_old', 30) if request.is_json else 30
        
        task_service = TaskService()
        deleted_count = task_service.cleanup_old_tasks(days_old)
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='task_management',
            action='cleanup_tasks',
            description=f'Limpieza de tareas antiguas ejecutada',
            metadata={
                'days_old': days_old,
                'deleted_count': deleted_count,
                'triggered_by': current_user.username
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': f'Se eliminaron {deleted_count} tareas antiguas',
            'deleted_count': deleted_count
        })
    
    except Exception as e:
        current_app.logger.error(f"Error cleaning up old tasks: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

# CSV File Management Endpoints

@api_bp.route('/csv/download/<int:request_id>')
@login_required
@admin_required
def download_request_csv(request_id):
    """Download CSV file for a specific permission request"""
    try:
        permission_request = PermissionRequest.query.get_or_404(request_id)
        
        # Generate CSV file if it doesn't exist
        csv_file_path = permission_request.generate_csv_file('add')
        
        if not os.path.exists(csv_file_path):
            return jsonify({
                'success': False,
                'message': 'Archivo CSV no encontrado'
            }), 404
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='file_access',
            action='download_csv',
            resource_type='permission_request',
            resource_id=permission_request.id,
            description=f'Descarga de CSV para solicitud #{permission_request.id}',
            metadata={
                'folder_path': permission_request.folder.path,
                'ad_group': permission_request.ad_group.name if permission_request.ad_group else 'Sin grupo',
                'permission_type': permission_request.permission_type,
                'csv_file_path': csv_file_path
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return send_file(
            csv_file_path,
            as_attachment=True,
            download_name=f"permission_change_{request_id}.csv",
            mimetype='text/csv'
        )
    
    except Exception as e:
        current_app.logger.error(f"Error downloading CSV for request {request_id}: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/csv/generate', methods=['POST'])
@login_required
@admin_required
def generate_bulk_csv():
    """Generate bulk CSV file for multiple permission changes"""
    try:
        data = request.get_json()
        if not data or 'changes' not in data:
            return jsonify({
                'success': False,
                'message': 'Se requiere una lista de cambios'
            }), 400
        
        changes = data['changes']
        if not changes:
            return jsonify({
                'success': False,
                'message': 'Lista de cambios vacía'
            }), 400
        
        # Validate changes format
        processed_changes = []
        for change in changes:
            if not all(key in change for key in ['request_id', 'action']):
                return jsonify({
                    'success': False,
                    'message': 'Formato de cambio inválido. Se requiere request_id y action.'
                }), 400
            
            if change['action'] not in ['add', 'remove']:
                return jsonify({
                    'success': False,
                    'message': 'Acción inválida. Debe ser "add" o "remove".'
                }), 400
            
            permission_request = PermissionRequest.query.get(change['request_id'])
            if not permission_request:
                return jsonify({
                    'success': False,
                    'message': f'Solicitud {change["request_id"]} no encontrada'
                }), 400
            
            processed_changes.append({
                'permission_request': permission_request,
                'action': change['action']
            })
        
        # Generate bulk CSV
        from app.services.csv_generator_service import CSVGeneratorService
        csv_service = CSVGeneratorService()
        csv_file_path = csv_service.generate_bulk_changes_csv(processed_changes)
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='file_creation',
            action='generate_bulk_csv',
            description=f'CSV masivo generado con {len(processed_changes)} cambios',
            metadata={
                'change_count': len(processed_changes),
                'request_ids': [change['permission_request'].id for change in processed_changes],
                'csv_file_path': csv_file_path
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': f'CSV generado exitosamente con {len(processed_changes)} cambios',
            'csv_file_path': csv_file_path,
            'download_url': f'/api/csv/download-file?path={csv_file_path}'
        })
    
    except Exception as e:
        current_app.logger.error(f"Error generating bulk CSV: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/csv/download-file')
@login_required
@admin_required
def download_csv_file():
    """Download a specific CSV file by path"""
    try:
        file_path = request.args.get('path')
        if not file_path:
            return jsonify({
                'success': False,
                'message': 'Ruta del archivo requerida'
            }), 400
        
        # Security check: only allow files from the CSV output directory
        csv_output_dir = current_app.config.get('CSV_OUTPUT_DIR', '/tmp/sar_csv_files')
        if not file_path.startswith(csv_output_dir):
            return jsonify({
                'success': False,
                'message': 'Acceso denegado a la ruta especificada'
            }), 403
        
        if not os.path.exists(file_path):
            return jsonify({
                'success': False,
                'message': 'Archivo no encontrado'
            }), 404
        
        if not file_path.endswith('.csv'):
            return jsonify({
                'success': False,
                'message': 'Solo se permiten archivos CSV'
            }), 400
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='file_access',
            action='download_csv_file',
            description=f'Descarga de archivo CSV: {os.path.basename(file_path)}',
            metadata={
                'csv_file_path': file_path,
                'file_size': os.path.getsize(file_path)
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return send_file(
            file_path,
            as_attachment=True,
            download_name=os.path.basename(file_path),
            mimetype='text/csv'
        )
    
    except Exception as e:
        current_app.logger.error(f"Error downloading CSV file: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/csv/list')
@login_required
@admin_required
def list_csv_files():
    """List available CSV files with metadata"""
    try:
        from app.services.csv_generator_service import CSVGeneratorService
        
        csv_service = CSVGeneratorService()
        csv_output_dir = csv_service.output_directory
        
        if not os.path.exists(csv_output_dir):
            return jsonify({
                'success': True,
                'files': []
            })
        
        files = []
        for filename in sorted(os.listdir(csv_output_dir)):
            if filename.endswith('.csv'):
                file_path = os.path.join(csv_output_dir, filename)
                file_info = csv_service.get_csv_file_info(file_path)
                if file_info:
                    files.append(file_info)
        
        return jsonify({
            'success': True,
            'files': files
        })
    
    except Exception as e:
        current_app.logger.error(f"Error listing CSV files: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/csv/cleanup', methods=['POST'])
@login_required
@admin_required
def cleanup_csv_files():
    """Clean up old CSV files"""
    try:
        data = request.get_json() if request.is_json else {}
        days_old = data.get('days_old', 7)
        
        from app.services.csv_generator_service import CSVGeneratorService
        csv_service = CSVGeneratorService()
        deleted_count = csv_service.cleanup_old_csv_files(days_old)
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='file_management',
            action='cleanup_csv_files',
            description=f'Limpieza de archivos CSV antiguos',
            metadata={
                'days_old': days_old,
                'deleted_count': deleted_count,
                'triggered_by': current_user.username
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': f'Se eliminaron {deleted_count} archivos CSV antiguos',
            'deleted_count': deleted_count
        })
    
    except Exception as e:
        current_app.logger.error(f"Error cleaning up CSV files: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/csv/generate-removal/<int:folder_id>/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def generate_removal_csv(folder_id, user_id):
    """Generate CSV file for removing user permissions from a folder"""
    try:
        data = request.get_json() if request.is_json else {}
        permission_type = data.get('permission_type', 'read')
        
        if permission_type not in ['read', 'write']:
            return jsonify({
                'success': False,
                'message': 'Tipo de permiso inválido. Debe ser "read" o "write".'
            }), 400
        
        from app.services.csv_generator_service import CSVGeneratorService
        csv_service = CSVGeneratorService()
        
        csv_file_path = csv_service.generate_removal_csv_from_folder_permissions(
            folder_id, user_id, permission_type
        )
        
        folder = Folder.query.get(folder_id)
        user = User.query.get(user_id)
        
        # Log audit event
        AuditEvent.log_event(
            user=current_user,
            event_type='file_creation',
            action='generate_removal_csv',
            description=f'CSV de eliminación generado para usuario {user.username} en carpeta {folder.name}',
            metadata={
                'folder_id': folder_id,
                'folder_path': folder.path,
                'user_id': user_id,
                'username': user.username,
                'permission_type': permission_type,
                'csv_file_path': csv_file_path
            },
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent')
        )
        
        return jsonify({
            'success': True,
            'message': f'CSV de eliminación generado exitosamente',
            'csv_file_path': csv_file_path,
            'download_url': f'/api/csv/download-file?path={csv_file_path}'
        })
    
    except ValueError as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 400
    except Exception as e:
        current_app.logger.error(f"Error generating removal CSV: {str(e)}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500

@api_bp.route('/check-existing-permissions', methods=['POST'])
@login_required
def check_existing_permissions():
    """API endpoint to check for existing permissions before creating a request"""
    try:
        data = request.get_json()
        if not data or not all(key in data for key in ['folder_id', 'permission_type']):
            return jsonify({
                'success': False,
                'message': 'Faltan datos requeridos (folder_id, permission_type)'
            }), 400
        
        folder_id = data['folder_id']
        permission_type = data['permission_type']
        
        # Use our new function to check existing permissions
        from app.models import PermissionRequest
        result = PermissionRequest.check_existing_permissions(
            current_user.id,
            folder_id,
            permission_type
        )
        
        return jsonify({
            'success': True,
            'result': result
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error checking existing permissions: {str(e)}")
        return jsonify({
            'success': False,
            'message': 'Error interno del servidor'
        }), 500