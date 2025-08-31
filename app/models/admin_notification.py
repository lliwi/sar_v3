from datetime import datetime, timedelta
from app import db
import hashlib

class AdminNotification(db.Model):
    __tablename__ = 'admin_notifications'
    
    id = db.Column(db.Integer, primary_key=True)
    error_hash = db.Column(db.String(64), nullable=False, index=True)  # Hash único del error
    error_type = db.Column(db.String(100), nullable=False)  # Tipo de error (airflow, ldap, email, etc.)
    error_message = db.Column(db.Text, nullable=False)  # Mensaje completo del error
    service_name = db.Column(db.String(100), nullable=False)  # Servicio que generó el error
    first_occurrence = db.Column(db.DateTime, default=datetime.utcnow)  # Primera vez que ocurrió
    last_occurrence = db.Column(db.DateTime, default=datetime.utcnow)  # Última vez que ocurrió
    occurrence_count = db.Column(db.Integer, default=1)  # Número de veces que ha ocurrido
    notification_sent = db.Column(db.Boolean, default=False)  # Si se envió notificación
    notification_sent_at = db.Column(db.DateTime)  # Cuándo se envió la notificación
    is_resolved = db.Column(db.Boolean, default=False)  # Si el error fue resuelto
    resolved_at = db.Column(db.DateTime)  # Cuándo se marcó como resuelto
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    @staticmethod
    def generate_error_hash(error_type, service_name, error_message):
        """Generate unique hash for error identification"""
        # Usar los primeros 500 caracteres del mensaje para evitar variaciones menores
        message_part = error_message[:500] if error_message else ""
        error_string = f"{error_type}:{service_name}:{message_part}"
        return hashlib.sha256(error_string.encode()).hexdigest()
    
    @staticmethod
    def should_notify(error_type, service_name, error_message, cooldown_hours=24):
        """Check if we should send notification for this error"""
        error_hash = AdminNotification.generate_error_hash(error_type, service_name, error_message)
        
        # Buscar notificación existente
        existing = AdminNotification.query.filter_by(error_hash=error_hash).first()
        
        if not existing:
            # Crear nueva entrada
            notification = AdminNotification(
                error_hash=error_hash,
                error_type=error_type,
                service_name=service_name,
                error_message=error_message
            )
            db.session.add(notification)
            db.session.commit()
            return True, notification
        else:
            # Actualizar ocurrencia
            existing.last_occurrence = datetime.utcnow()
            existing.occurrence_count += 1
            existing.updated_at = datetime.utcnow()
            
            # Si no se resolvió y han pasado las horas de cooldown desde la última notificación
            if not existing.is_resolved and existing.notification_sent:
                cooldown_passed = (
                    not existing.notification_sent_at or 
                    existing.notification_sent_at + timedelta(hours=cooldown_hours) <= datetime.utcnow()
                )
                if cooldown_passed:
                    db.session.commit()
                    return True, existing
            elif not existing.notification_sent:
                db.session.commit()
                return True, existing
            
            db.session.commit()
            return False, existing
    
    @staticmethod
    def mark_notification_sent(notification_id):
        """Mark notification as sent"""
        notification = AdminNotification.query.get(notification_id)
        if notification:
            notification.notification_sent = True
            notification.notification_sent_at = datetime.utcnow()
            db.session.commit()
    
    @staticmethod
    def mark_resolved(error_type, service_name, error_message):
        """Mark error as resolved"""
        error_hash = AdminNotification.generate_error_hash(error_type, service_name, error_message)
        notification = AdminNotification.query.filter_by(error_hash=error_hash).first()
        if notification:
            notification.is_resolved = True
            notification.resolved_at = datetime.utcnow()
            db.session.commit()
    
    @staticmethod
    def cleanup_old_notifications(days=30):
        """Clean up old resolved notifications"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        AdminNotification.query.filter(
            AdminNotification.is_resolved == True,
            AdminNotification.resolved_at < cutoff_date
        ).delete()
        db.session.commit()
    
    def to_dict(self):
        return {
            'id': self.id,
            'error_type': self.error_type,
            'service_name': self.service_name,
            'error_message': self.error_message,
            'first_occurrence': self.first_occurrence.isoformat() if self.first_occurrence else None,
            'last_occurrence': self.last_occurrence.isoformat() if self.last_occurrence else None,
            'occurrence_count': self.occurrence_count,
            'notification_sent': self.notification_sent,
            'is_resolved': self.is_resolved
        }