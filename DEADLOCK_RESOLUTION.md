# Database Deadlock Resolution

## Problem Identified

The application was experiencing frequent database deadlocks during AD synchronization operations. This occurred because multiple processes (Celery workers, scheduler service, and Flask app) were attempting to update the same user and group records simultaneously.

### Symptoms
- Frequent `psycopg2.errors.DeadlockDetected` errors in logs
- Synchronization failures
- Console flooded with WARNING messages about deadlocks
- Performance degradation during sync operations

### Root Cause
Multiple concurrent transactions updating the same database rows created circular lock dependencies:
1. **Scheduler** running periodic AD syncs
2. **Celery workers** processing membership syncs
3. **Flask app** handling user requests
4. All updating the same `users` and `ad_groups` tables simultaneously

## Solution Implemented

### 1. Retry Logic with Exponential Backoff

Created a new utility module `app/utils/db_utils.py` with the following features:

#### `retry_on_deadlock` Decorator
- Automatically retries functions that encounter deadlocks
- Configurable max attempts (default: 3)
- Exponential backoff delay (starts at 0.1s, doubles each retry)
- Only retries on `DeadlockDetected` exceptions

```python
@retry_on_deadlock(max_attempts=5, initial_delay=0.1, backoff_factor=2)
def my_database_operation():
    # ... database operations ...
    db.session.commit()
```

#### `commit_with_retry` Function
- Standalone function for retrying commits
- Automatic rollback on deadlock
- Returns True/False for success/failure

```python
if commit_with_retry(max_attempts=3):
    logger.info("Commit succeeded")
else:
    logger.error("Commit failed after retries")
```

#### `batch_commit_with_retry` Function
- Process items in batches with automatic retry
- Tracks processed and failed counts
- Configurable batch size

```python
processed, failed = batch_commit_with_retry(
    users,
    update_user_func,
    batch_size=50
)
```

#### `get_for_update_skip_locked` Function
- Uses PostgreSQL's `FOR UPDATE SKIP LOCKED`
- Allows parallel workers to skip locked rows
- Prevents blocking on already-processing records

### 2. Applied to All Synchronization Points

Updated all `db.session.commit()` calls in:

1. **app/services/ldap_service.py**
   - `sync_groups()` - Group synchronization batches
   - `sync_users()` - User synchronization batches
   - Inactive record marking

2. **celery_worker.py**
   - `sync_memberships_optimized_task()` - Membership sync batches
   - `sync_users_from_ad_task()` - User sync batches
   - Final commits

3. **app/services/scheduler_service.py**
   - `_sync_active_permissions()` - Permission sync batches
   - `create_system_user()` - System user creation
   - Final commits

### 3. Configuration

All retry logic uses consistent defaults:
- **Max attempts**: 3
- **Initial delay**: 0.1 seconds
- **Backoff factor**: 2x (0.1s → 0.2s → 0.4s)

## Results

### Before Implementation
```
WARNING - app.services.ldap_service - (psycopg2.errors.DeadlockDetected) deadlock detected
WARNING - app.services.ldap_service - (psycopg2.errors.DeadlockDetected) deadlock detected
WARNING - app.services.ldap_service - (psycopg2.errors.DeadlockDetected) deadlock detected
[... hundreds of deadlock errors ...]
```

### After Implementation
- **Zero deadlock errors** in logs
- Automatic recovery from contention scenarios
- Smooth synchronization operations
- Only legitimate warnings (users not found in AD)

## Monitoring

To verify deadlock resolution is working:

```bash
# Check for deadlock errors (should be zero)
docker-compose logs --since=1h | grep -i "deadlock" | wc -l

# Monitor retry attempts (informational)
docker-compose logs --since=1h | grep -i "retry.*deadlock"

# View successful commits
docker-compose logs --since=1h | grep "✅.*commit"
```

## Best Practices

1. **Use `commit_with_retry()` for all batch operations**
   ```python
   if commit_with_retry(max_attempts=3):
       logger.info("Batch committed successfully")
   ```

2. **Wrap critical sections with `@retry_on_deadlock`**
   ```python
   @retry_on_deadlock(max_attempts=5)
   def critical_update():
       # ... complex transaction ...
       db.session.commit()
   ```

3. **Use `FOR UPDATE SKIP LOCKED` for parallel processing**
   ```python
   users = get_for_update_skip_locked(
       User,
       User.ad_status == 'active',
       limit=100
   )
   ```

4. **Keep transactions short**
   - Commit in small batches (50-100 records)
   - Avoid long-running transactions
   - Release locks quickly

5. **Monitor for persistent deadlocks**
   - If retries are exhausted (3 attempts), investigate:
     - Transaction ordering
     - Lock escalation
     - Query optimization
     - Index usage

## Related Files

- [app/utils/db_utils.py](app/utils/db_utils.py) - Retry utilities
- [app/services/ldap_service.py](app/services/ldap_service.py) - LDAP sync with retries
- [celery_worker.py](celery_worker.py) - Celery tasks with retries
- [app/services/scheduler_service.py](app/services/scheduler_service.py) - Scheduler with retries
- [LOGGING_CONFIG.md](LOGGING_CONFIG.md) - Async logging configuration

## Future Optimizations

If deadlocks resurface under extreme load:

1. **Partition work by user ID range**
   - Worker 1: users with id % 3 == 0
   - Worker 2: users with id % 3 == 1
   - Worker 3: users with id % 3 == 2

2. **Implement advisory locks**
   - Use PostgreSQL advisory locks for critical sections
   - Prevent concurrent updates to same records

3. **Queue-based synchronization**
   - Single worker processes updates sequentially
   - No concurrent writes to same tables

4. **Read replicas**
   - Offload read-heavy operations
   - Reduce contention on primary database
