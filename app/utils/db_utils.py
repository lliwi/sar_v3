"""
Database utility functions for handling transactions and deadlocks
"""
import time
import logging
from functools import wraps
from sqlalchemy.exc import OperationalError
from psycopg2.errors import DeadlockDetected
from app import db

logger = logging.getLogger(__name__)


def retry_on_deadlock(max_attempts=3, initial_delay=0.1, backoff_factor=2):
    """
    Decorator to retry database operations on deadlock with exponential backoff

    Args:
        max_attempts: Maximum number of retry attempts (default 3)
        initial_delay: Initial delay in seconds before first retry (default 0.1)
        backoff_factor: Multiplier for delay on each retry (default 2)

    Usage:
        @retry_on_deadlock(max_attempts=5)
        def my_db_operation():
            # ... database operations ...
            db.session.commit()
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            delay = initial_delay

            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)

                except OperationalError as e:
                    # Check if it's a deadlock error
                    if isinstance(e.orig, DeadlockDetected):
                        attempt += 1

                        if attempt >= max_attempts:
                            logger.error(
                                f"❌ Deadlock persisted after {max_attempts} attempts in {func.__name__}"
                            )
                            raise

                        # Rollback the failed transaction
                        db.session.rollback()

                        logger.warning(
                            f"🔄 Deadlock detected in {func.__name__}, "
                            f"retry {attempt}/{max_attempts} after {delay:.2f}s"
                        )

                        # Wait before retrying with exponential backoff
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        # Not a deadlock, re-raise the error
                        raise

                except Exception as e:
                    # For any other exception, rollback and re-raise
                    db.session.rollback()
                    raise

            return None

        return wrapper
    return decorator


def commit_with_retry(session=None, max_attempts=3, initial_delay=0.1):
    """
    Commit a database session with automatic retry on deadlock

    Args:
        session: SQLAlchemy session (defaults to db.session)
        max_attempts: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry

    Returns:
        bool: True if commit succeeded, False otherwise
    """
    if session is None:
        session = db.session

    attempt = 0
    delay = initial_delay

    while attempt < max_attempts:
        try:
            session.commit()
            return True

        except OperationalError as e:
            if isinstance(e.orig, DeadlockDetected):
                attempt += 1

                if attempt >= max_attempts:
                    logger.error(f"❌ Commit failed after {max_attempts} deadlock attempts")
                    session.rollback()
                    return False

                session.rollback()
                logger.warning(
                    f"🔄 Deadlock on commit, retry {attempt}/{max_attempts} after {delay:.2f}s"
                )

                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logger.error(f"❌ Database error on commit: {str(e)}")
                session.rollback()
                raise

        except Exception as e:
            logger.error(f"❌ Unexpected error on commit: {str(e)}")
            session.rollback()
            raise

    return False


def batch_commit_with_retry(items, process_func, batch_size=100, max_attempts=3):
    """
    Process items in batches with commit retry logic

    Args:
        items: Iterable of items to process
        process_func: Function to process each item (takes item as argument)
        batch_size: Number of items per batch commit
        max_attempts: Maximum retry attempts for deadlocks

    Returns:
        tuple: (processed_count, failed_count)

    Usage:
        def update_user(user):
            user.last_sync = datetime.utcnow()

        processed, failed = batch_commit_with_retry(users, update_user, batch_size=50)
    """
    processed_count = 0
    failed_count = 0
    batch_count = 0

    for i, item in enumerate(items):
        try:
            # Process the item
            process_func(item)
            batch_count += 1

            # Commit in batches
            if batch_count >= batch_size or i == len(items) - 1:
                if commit_with_retry(max_attempts=max_attempts):
                    processed_count += batch_count
                    logger.debug(f"✅ Batch committed: {batch_count} items")
                    batch_count = 0
                else:
                    failed_count += batch_count
                    batch_count = 0

        except Exception as e:
            logger.error(f"❌ Error processing item: {str(e)}")
            failed_count += 1
            db.session.rollback()
            batch_count = 0

    return processed_count, failed_count


def get_for_update_skip_locked(model, filter_conditions, limit=None):
    """
    Get records with FOR UPDATE SKIP LOCKED to avoid blocking on locked rows

    This is useful for parallel processing where multiple workers should
    skip rows that are currently being processed by others.

    Args:
        model: SQLAlchemy model class
        filter_conditions: SQLAlchemy filter conditions
        limit: Optional limit on number of rows

    Returns:
        Query result

    Usage:
        users = get_for_update_skip_locked(
            User,
            User.ad_status == 'active',
            limit=100
        )
    """
    query = model.query.filter(filter_conditions).with_for_update(skip_locked=True)

    if limit:
        query = query.limit(limit)

    return query.all()
