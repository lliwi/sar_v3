#!/usr/bin/env python3
"""
Standalone scheduler service for AD synchronization
Runs independently from the main Flask application to avoid multi-process conflicts
"""

import os
import sys
import time
import signal
import logging
from datetime import datetime

# Add the app directory to Python path
sys.path.insert(0, '/app')

from app import create_app
from app.services.scheduler_service import SchedulerService

# Configure asynchronous logging with QueueHandler
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler
import queue
import atexit

log_level = os.getenv('LOG_LEVEL', 'INFO').upper()

# Create queue for async logging (non-blocking)
log_queue = queue.Queue(-1)
queue_handler = QueueHandler(log_queue)

# Real handlers
handlers = []

# File handler with rotation (if enabled)
if os.path.exists('/app/logs') and os.getenv('LOG_TO_FILE', 'true').lower() == 'true':
    file_handler = RotatingFileHandler(
        '/app/logs/scheduler.log',
        maxBytes=int(os.getenv('LOG_MAX_BYTES', 10*1024*1024)),
        backupCount=int(os.getenv('LOG_BACKUP_COUNT', 5)),
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    handlers.append(file_handler)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, log_level))
handlers.append(console_handler)

# Format
if log_level == 'DEBUG':
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
else:
    log_format = '%(levelname)s - %(name)s - %(message)s'

formatter = logging.Formatter(log_format)
for handler in handlers:
    handler.setFormatter(formatter)

# Queue listener
queue_listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
queue_listener.start()
atexit.register(queue_listener.stop)

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, log_level))
# Clear any existing handlers to avoid duplicates
root_logger.handlers.clear()
root_logger.addHandler(queue_handler)

# Clear handlers from all existing loggers
for name in logging.root.manager.loggerDict:
    logger_obj = logging.getLogger(name)
    logger_obj.handlers.clear()
    logger_obj.propagate = True

# Suppress noisy loggers
if log_level != 'DEBUG':
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy').setLevel(logging.WARNING)
    logging.getLogger('ldap3').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler_service = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down scheduler...")
    if scheduler_service:
        scheduler_service.stop()
    sys.exit(0)

def main():
    """Main scheduler service entry point"""
    global scheduler_service

    logger.info("Starting standalone AD synchronization scheduler service")

    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Create Flask app context
        app = create_app()

        with app.app_context():
            logger.info("Flask app context created for scheduler service")

            # Create scheduler service instance
            scheduler_service = SchedulerService()

            # Start the scheduler service
            scheduler_service.start(app)

            # Keep the main thread alive
            while scheduler_service.running:
                time.sleep(10)  # Sleep for 10 seconds, then check again

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        if scheduler_service:
            scheduler_service.stop()
    except Exception as e:
        logger.error(f"Unexpected error in scheduler service: {str(e)}")
        if scheduler_service:
            scheduler_service.stop()
        sys.exit(1)

if __name__ == '__main__':
    main()