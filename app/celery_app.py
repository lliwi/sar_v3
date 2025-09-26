from celery import Celery

def make_celery(app=None):
    """Create and configure Celery instance"""
    if app is None:
        from app import create_app
        app = create_app()
    
    celery = Celery(
        app.import_name,
        backend=app.config.get('result_backend', 'redis://localhost:6379/0'),
        broker=app.config.get('broker_url', 'redis://localhost:6379/0')
    )
    
    celery.conf.update(app.config)
    celery.conf.broker_connection_retry_on_startup = True

    # Configure specialized queues
    celery.conf.task_routes = {
        # Sync tasks - Heavy operations
        'celery_worker.sync_users_from_ad_task': {'queue': 'sync_heavy'},
        'celery_worker.sync_memberships_optimized_task': {'queue': 'sync_heavy'},

        # Email notifications - Fast processing
        'celery_worker.send_permission_request_notification': {'queue': 'notifications'},
        'celery_worker.send_permission_status_notification': {'queue': 'notifications'},

        # Reports and exports - Medium priority (reserved for future use)
        'generate_report_task': {'queue': 'reports'},
        'export_permissions_task': {'queue': 'reports'},

        # Default queue for other tasks
        '*': {'queue': 'default'},
    }

    # Queue configuration
    celery.conf.task_default_queue = 'default'
    celery.conf.task_queues = {
        'sync_heavy': {
            'exchange': 'sync_heavy',
            'routing_key': 'sync_heavy',
        },
        'notifications': {
            'exchange': 'notifications',
            'routing_key': 'notifications',
        },
        'reports': {
            'exchange': 'reports',
            'routing_key': 'reports',
        },
        'default': {
            'exchange': 'default',
            'routing_key': 'default',
        }
    }
    
    class ContextTask(celery.Task):
        """Make celery tasks work with Flask app context."""
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)
    
    celery.Task = ContextTask
    return celery

# Create a basic Celery instance without Flask app (to be configured later)
celery = Celery('app')