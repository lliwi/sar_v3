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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

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