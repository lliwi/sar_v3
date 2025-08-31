import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask import current_app, url_for, render_template
from app.models import PermissionRequest, User
from app.views.api import generate_validation_token
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class EmailService:
    def __init__(self):
        self.smtp_server = current_app.config.get('SMTP_SERVER')
        self.smtp_port = current_app.config.get('SMTP_PORT', 587)
        self.smtp_username = current_app.config.get('SMTP_USERNAME')
        self.smtp_password = current_app.config.get('SMTP_PASSWORD')
        self.smtp_use_tls = current_app.config.get('SMTP_USE_TLS', True)
        self.smtp_from = current_app.config.get('SMTP_FROM', self.smtp_username)
    
    def send_email(self, to_email, subject, html_body, text_body=None):
        """Send email using SMTP"""
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_from
            msg['To'] = to_email
            
            # Add text and HTML parts
            if text_body:
                text_part = MIMEText(text_body, 'plain', 'utf-8')
                msg.attach(text_part)
            
            html_part = MIMEText(html_body, 'html', 'utf-8')
            msg.attach(html_part)
            
            # Connect to server and send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                if self.smtp_use_tls:
                    server.starttls()
                
                # Only authenticate if username and password are provided
                if self.smtp_username and self.smtp_password:
                    server.login(self.smtp_username, self.smtp_password)
                
                server.send_message(msg)
            
            logger.info(f"Email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending email to {to_email}: {str(e)}")
            return False
    
    def generate_permission_request_email(self, permission_request, validator):
        """Generate email content for permission request notification"""
        token = generate_validation_token(permission_request.id)
        
        # Generate validation links using SERVER_URL configuration
        server_url = current_app.config.get('SERVER_URL') or current_app.config.get('BASE_URL', 'http://localhost:8080')
        approve_url = f"{server_url}/api/validate-permission/{permission_request.id}/{token}?action=approve"
        reject_url = f"{server_url}/api/validate-permission/{permission_request.id}/{token}?action=reject"
        web_url = f"{server_url}/validate-request/{permission_request.id}"
        
        subject = f"Solicitud de Permiso Pendiente - {permission_request.folder.path}"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #007bff; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
                .content {{ background-color: #f8f9fa; padding: 20px; }}
                .details {{ background-color: white; padding: 15px; margin: 10px 0; border-radius: 5px; border: 1px solid #dee2e6; }}
                .buttons {{ text-align: center; margin: 20px 0; }}
                .btn {{ display: inline-block; padding: 12px 24px; margin: 0 10px; text-decoration: none; border-radius: 5px; font-weight: bold; }}
                .btn-approve {{ background-color: #28a745; color: white; }}
                .btn-reject {{ background-color: #dc3545; color: white; }}
                .btn-web {{ background-color: #007bff; color: white; }}
                .footer {{ background-color: #e9ecef; padding: 15px; border-radius: 0 0 5px 5px; font-size: 12px; color: #6c757d; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>Nueva Solicitud de Permiso</h2>
                </div>
                
                <div class="content">
                    <p>Estimado/a {validator.full_name},</p>
                    
                    <p>Se ha recibido una nueva solicitud de permiso que requiere su validación:</p>
                    
                    <div class="details">
                        <strong>Solicitante:</strong> {permission_request.requester.full_name} ({permission_request.requester.username})<br>
                        <strong>Carpeta:</strong> {permission_request.folder.path}<br>
                        <strong>Grupo AD:</strong> {permission_request.ad_group.name}<br>
                        <strong>Tipo de Permiso:</strong> {permission_request.permission_type.title()}<br>
                        <strong>Fecha de Solicitud:</strong> {permission_request.created_at.strftime('%d/%m/%Y %H:%M')}<br>
                        
                        <div style="margin-top: 15px;">
                            <strong>Justificación:</strong><br>
                            {permission_request.justification}
                        </div>
                        
                        <div style="margin-top: 15px;">
                            <strong>Necesidad de Negocio:</strong><br>
                            {permission_request.business_need}
                        </div>
                    </div>
                    
                    <div class="buttons">
                        <a href="{approve_url}" class="btn btn-approve">✓ Aprobar</a>
                        <a href="{reject_url}" class="btn btn-reject">✗ Rechazar</a>
                    </div>
                    
                    <p style="text-align: center;">
                        O puede revisar la solicitud en el sistema web:
                    </p>
                    
                    <div class="buttons">
                        <a href="{web_url}" class="btn btn-web">Ver en Sistema Web</a>
                    </div>
                </div>
                
                <div class="footer">
                    <p>Este correo fue generado automáticamente por el Sistema de Gestión de Permisos de Carpetas.</p>
                    <p>Por favor, no responda a este correo electrónico.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Nueva Solicitud de Permiso
        
        Estimado/a {validator.full_name},
        
        Se ha recibido una nueva solicitud de permiso que requiere su validación:
        
        Solicitante: {permission_request.requester.full_name} ({permission_request.requester.username})
        Carpeta: {permission_request.folder.path}
        Grupo AD: {permission_request.ad_group.name}
        Tipo de Permiso: {permission_request.permission_type.title()}
        Fecha de Solicitud: {permission_request.created_at.strftime('%d/%m/%Y %H:%M')}
        
        Justificación:
        {permission_request.justification}
        
        Necesidad de Negocio:
        {permission_request.business_need}
        
        Para aprobar la solicitud: {approve_url}
        Para rechazar la solicitud: {reject_url}
        
        O puede revisar la solicitud en el sistema web: {web_url}
        
        ---
        Este correo fue generado automáticamente por el Sistema de Gestión de Permisos de Carpetas.
        Por favor, no responda a este correo electrónico.
        """
        
        return subject, html_body, text_body
    
    def generate_permission_request_email_html(self, permission_request, validator):
        """Generate email content using HTML templates"""
        token = generate_validation_token(permission_request.id)
        
        # Generate validation links using SERVER_URL configuration
        server_url = current_app.config.get('SERVER_URL') or current_app.config.get('BASE_URL', 'http://localhost:8080')
        approve_url = f"{server_url}/api/validate-permission/{permission_request.id}/{token}?action=approve"
        reject_url = f"{server_url}/api/validate-permission/{permission_request.id}/{token}?action=reject"
        web_url = f"{server_url}/validate-request/{permission_request.id}"
        
        subject = f"Solicitud de Permiso Pendiente - {permission_request.folder.path}"
        
        # Render HTML template
        html_body = render_template(
            'email/validation_request.html',
            permission_request=permission_request,
            validator=validator,
            approve_url=approve_url,
            reject_url=reject_url,
            web_url=web_url
        )
        
        # Generate text version
        text_body = f"""
Nueva Solicitud de Permiso

Estimado/a {validator.full_name},

Se ha recibido una nueva solicitud de permiso que requiere su validación:

Solicitante: {permission_request.requester.full_name} ({permission_request.requester.username})
Carpeta: {permission_request.folder.path}
Grupo AD: {permission_request.ad_group.name}
Tipo de Permiso: {permission_request.permission_type.title()}
Fecha de Solicitud: {permission_request.created_at.strftime('%d/%m/%Y %H:%M')}

Justificación:
{permission_request.justification}

Necesidad de Negocio:
{permission_request.business_need}

Para aprobar la solicitud: {approve_url}
Para rechazar la solicitud: {reject_url}

O puede revisar la solicitud en el sistema web: {web_url}

---
Este correo fue generado automáticamente por el Sistema de Gestión de Permisos de Carpetas.
Por favor, no responda a este correo electrónico.
        """
        
        return subject, html_body, text_body
    
    def generate_status_notification_email_html(self, permission_request, status):
        """Generate status notification email using HTML templates"""
        subject = f"Solicitud de Permiso {'Aprobada' if status == 'approved' else 'Rechazada'} - {permission_request.folder.path}"
        
        # Get server URL for links
        server_url = current_app.config.get('SERVER_URL') or current_app.config.get('BASE_URL', 'http://localhost:8080')
        
        # Render HTML template
        html_body = render_template(
            'email/request_status_notification.html',
            permission_request=permission_request,
            status=status,
            status_text='Aprobada' if status == 'approved' else 'Rechazada',
            base_url=server_url
        )
        
        # Generate text version
        status_text = "Aprobada" if status == "approved" else "Rechazada"
        text_body = f"""
Solicitud de Permiso {status_text}

Estimado/a {permission_request.requester.full_name},

Su solicitud de permiso ha sido {status_text.lower()}:

Carpeta: {permission_request.folder.path}
Grupo AD: {permission_request.ad_group.name}
Tipo de Permiso: {permission_request.permission_type.title()}
Validado por: {permission_request.validator.full_name if permission_request.validator else 'Sistema'}
Fecha de Validación: {permission_request.validation_date.strftime('%d/%m/%Y %H:%M') if permission_request.validation_date else 'N/A'}

{f'Comentario del Validador: {permission_request.validation_comment}' if permission_request.validation_comment else ''}

{f'Los cambios serán aplicados en Active Directory próximamente.' if status == 'approved' else ''}

Ver todas mis solicitudes: {server_url}/my-requests

---
Este correo fue generado automáticamente por el Sistema de Gestión de Permisos de Carpetas.
Por favor, no responda a este correo electrónico.
        """
        
        return subject, html_body, text_body
    
    def generate_admin_error_notification_email(self, notification):
        """Generate admin error notification email"""
        subject = f"[SAR System] Error en {notification.service_name} - {notification.error_type}"
        
        # Get server URL
        server_url = current_app.config.get('SERVER_URL') or current_app.config.get('BASE_URL', 'http://localhost:8080')
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 700px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #dc3545; color: white; padding: 20px; border-radius: 5px 5px 0 0; }}
                .content {{ background-color: #f8f9fa; padding: 20px; }}
                .error-details {{ background-color: white; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #dc3545; }}
                .stats {{ background-color: #e9ecef; padding: 10px; margin: 10px 0; border-radius: 5px; }}
                .footer {{ background-color: #e9ecef; padding: 15px; border-radius: 0 0 5px 5px; font-size: 12px; color: #6c757d; }}
                .severity-high {{ color: #dc3545; font-weight: bold; }}
                pre {{ background-color: #f1f3f4; padding: 10px; border-radius: 3px; overflow-x: auto; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>🚨 Error del Sistema SAR</h2>
                    <p>Se ha detectado un error en el sistema que requiere atención</p>
                </div>
                
                <div class="content">
                    <div class="error-details">
                        <h3>Detalles del Error</h3>
                        <p><strong>Servicio:</strong> {notification.service_name}</p>
                        <p><strong>Tipo:</strong> <span class="severity-high">{notification.error_type}</span></p>
                        <p><strong>Primera Ocurrencia:</strong> {notification.first_occurrence.strftime('%d/%m/%Y %H:%M:%S')}</p>
                        <p><strong>Última Ocurrencia:</strong> {notification.last_occurrence.strftime('%d/%m/%Y %H:%M:%S')}</p>
                        
                        <div class="stats">
                            <strong>Estadísticas:</strong><br>
                            • Número de ocurrencias: {notification.occurrence_count}<br>
                            • Hash del error: <code>{notification.error_hash[:16]}...</code>
                        </div>
                        
                        <h4>Mensaje de Error:</h4>
                        <pre>{notification.error_message}</pre>
                    </div>
                    
                    <div class="error-details">
                        <h3>Recomendaciones</h3>
                        <ul>
                            <li>Revisar los logs del servicio <strong>{notification.service_name}</strong></li>
                            <li>Verificar conectividad y configuración</li>
                            <li>Comprobar el estado de los servicios dependientes</li>
                            <li>Si el error persiste, considere reiniciar el servicio</li>
                        </ul>
                    </div>
                    
                    <p style="margin-top: 20px; padding: 10px; background-color: #fff3cd; border: 1px solid #ffeaa7; border-radius: 5px;">
                        <strong>⚠️ Importante:</strong> Esta notificación se enviará solo una vez cada 24 horas para el mismo error, 
                        a menos que se marque como resuelto.
                    </p>
                </div>
                
                <div class="footer">
                    <p>Sistema de Gestión de Permisos de Carpetas SAR</p>
                    <p>Servidor: {server_url}</p>
                    <p>Timestamp: {datetime.utcnow().strftime('%d/%m/%Y %H:%M:%S')} UTC</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        ERROR DEL SISTEMA SAR
        =====================
        
        Se ha detectado un error en el sistema que requiere atención:
        
        DETALLES DEL ERROR:
        - Servicio: {notification.service_name}
        - Tipo: {notification.error_type}
        - Primera Ocurrencia: {notification.first_occurrence.strftime('%d/%m/%Y %H:%M:%S')}
        - Última Ocurrencia: {notification.last_occurrence.strftime('%d/%m/%Y %H:%M:%S')}
        - Número de ocurrencias: {notification.occurrence_count}
        - Hash del error: {notification.error_hash[:16]}...
        
        MENSAJE DE ERROR:
        {notification.error_message}
        
        RECOMENDACIONES:
        - Revisar los logs del servicio {notification.service_name}
        - Verificar conectividad y configuración
        - Comprobar el estado de los servicios dependientes
        - Si el error persiste, considere reiniciar el servicio
        
        IMPORTANTE: Esta notificación se enviará solo una vez cada 24 horas para el mismo error.
        
        ---
        Sistema de Gestión de Permisos de Carpetas SAR
        Servidor: {server_url}
        Timestamp: {datetime.utcnow().strftime('%d/%m/%Y %H:%M:%S')} UTC
        """
        
        return subject, html_body, text_body

def send_admin_error_notification(error_type, service_name, error_message, cooldown_hours=24):
    """Send error notification to administrators if not already sent recently"""
    try:
        from app import create_app
        from app.models.admin_notification import AdminNotification
        
        app = create_app()
        
        with app.app_context():
            # Check if we should send notification
            should_notify, notification = AdminNotification.should_notify(
                error_type, service_name, error_message, cooldown_hours
            )
            
            if not should_notify:
                logger.info(f"Skipping duplicate notification for {error_type} in {service_name}")
                return False
            
            # Check if admin notifications are enabled
            if not current_app.config.get('ADMIN_NOTIFICATION_ENABLED', True):
                logger.info("Admin notifications disabled in configuration")
                return False
            
            admin_email = current_app.config.get('ADMIN_EMAIL')
            if not admin_email:
                logger.warning("ADMIN_EMAIL not configured, cannot send admin notification")
                return False
            
            email_service = EmailService()
            
            # Generate email content
            subject, html_body, text_body = email_service.generate_admin_error_notification_email(notification)
            
            # Send email
            success = email_service.send_email(admin_email, subject, html_body, text_body)
            
            if success:
                AdminNotification.mark_notification_sent(notification.id)
                logger.info(f"Admin error notification sent to {admin_email} for {error_type} in {service_name}")
            else:
                logger.error(f"Failed to send admin error notification to {admin_email}")
            
            return success
            
    except Exception as e:
        logger.error(f"Error sending admin error notification: {str(e)}")
        return False

def send_permission_request_notification(request_id):
    """Celery task to send permission request notification email"""
    try:
        from app import create_app
        app = create_app()
        
        with app.app_context():
            permission_request = PermissionRequest.query.get(request_id)
            if not permission_request:
                logger.error(f"Permission request {request_id} not found")
                return False
            
            email_service = EmailService()
            
            # Get validators for the folder
            validators = []
            
            # Add folder owners
            validators.extend(permission_request.folder.owners)
            
            # Add folder validators
            validators.extend(permission_request.folder.validators)
            
            # Remove duplicates
            validators = list(set(validators))
            
            if not validators:
                logger.warning(f"No validators found for folder {permission_request.folder.path}")
                return False
            
            success_count = 0
            for validator in validators:
                if validator.email:
                    subject, html_body, text_body = email_service.generate_permission_request_email_html(
                        permission_request, validator
                    )
                    
                    if email_service.send_email(validator.email, subject, html_body, text_body):
                        success_count += 1
                    else:
                        logger.error(f"Failed to send email to {validator.email}")
            
            logger.info(f"Sent {success_count} notification emails for request {request_id}")
            return success_count > 0
            
    except Exception as e:
        logger.error(f"Error sending permission request notification: {str(e)}")
        return False

def send_permission_status_notification(request_id, status):
    """Celery task to send permission status change notification"""
    try:
        from app import create_app
        app = create_app()
        
        with app.app_context():
            permission_request = PermissionRequest.query.get(request_id)
            if not permission_request:
                logger.error(f"Permission request {request_id} not found")
                return False
            
            email_service = EmailService()
            
            # Send notification to requester
            requester = permission_request.requester
            if not requester.email:
                logger.warning(f"No email found for requester {requester.username}")
                return False
            
            # Generate email using HTML template
            subject, html_body, text_body = email_service.generate_status_notification_email_html(
                permission_request, status
            )
            
            success = email_service.send_email(requester.email, subject, html_body, text_body)
            
            if success:
                logger.info(f"Status notification sent to {requester.email} for request {request_id}")
            else:
                logger.error(f"Failed to send status notification to {requester.email}")
            
            return success
            
    except Exception as e:
        logger.error(f"Error sending permission status notification: {str(e)}")
        return False