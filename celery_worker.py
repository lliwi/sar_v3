#!/usr/bin/env python3

from app.celery_app import make_celery
from app import create_app
from app.services.email_service import send_permission_request_notification as _send_permission_request_notification
from app.services.email_service import send_permission_status_notification as _send_permission_status_notification

# Create Flask app and configure Celery
app = create_app()
celery = make_celery(app)

# Register tasks
@celery.task
def send_permission_request_notification(request_id):
    """Celery task wrapper for sending permission request notification email"""
    return _send_permission_request_notification(request_id)

@celery.task
def send_permission_status_notification(request_id, status):
    """Celery task wrapper for sending permission status change notification"""
    return _send_permission_status_notification(request_id, status)