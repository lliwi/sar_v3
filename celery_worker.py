#!/usr/bin/env python3

from app.celery_app import make_celery
from app import create_app
from app.services.email_service import send_permission_request_notification as _send_permission_request_notification
from app.services.email_service import send_permission_status_notification as _send_permission_status_notification

# Create Flask app and configure Celery
app = create_app()
celery = make_celery(app)

# Ensure celery instance is available as module attribute
app.celery = celery

# Import all tasks to ensure they're registered
# This is needed because the worker needs explicit imports
import sys
import os

# Add the project root to Python path if not already there
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Register email tasks
@celery.task(queue='notifications', name='celery_worker.send_permission_request_notification')
def send_permission_request_notification(request_id):
    """Celery task wrapper for sending permission request notification email"""
    return _send_permission_request_notification(request_id)

@celery.task(queue='notifications', name='celery_worker.send_permission_status_notification')
def send_permission_status_notification(request_id, status):
    """Celery task wrapper for sending permission status change notification"""
    return _send_permission_status_notification(request_id, status)

@celery.task(bind=True, queue='sync_heavy', name='celery_worker.sync_memberships_optimized_task')
def sync_memberships_optimized_task(self, user_id):
    """
    Optimized task to sync ONLY memberships, assumes users are already synchronized
    Searches for missing users on demand with intelligent fallback
    """
    try:
        with app.app_context():
            from app.services.ldap_service import LDAPService
            from app.models import Folder, User, UserADGroupMembership, ADGroup, AuditEvent
            from app import db
            from datetime import datetime
            import os
            import logging

            logger = logging.getLogger(__name__)
            logger.info(f"🚀 Starting OPTIMIZED membership sync task - Task ID: {self.request.id}")

            # Get configuration from environment
            max_fallback_lookups = float('inf')  # No limit - process all users
            batch_size_groups = int(os.getenv('MEMBERSHIP_SYNC_BATCH_SIZE_GROUPS', 10))
            enable_fallback = os.getenv('MEMBERSHIP_SYNC_ENABLE_FALLBACK', 'true').lower() == 'true'
            batch_size_commits = int(os.getenv('BACKGROUND_SYNC_BATCH_SIZE', 25))

            logger.info(f"📋 Configuration: max_fallback=unlimited, batch_groups={batch_size_groups}, fallback={enable_fallback}")

            # Get requesting user
            requesting_user = User.query.get(user_id)
            if not requesting_user:
                raise Exception(f"User with ID {user_id} not found")

            # 1. STEP 1: Cache all existing users (1 DB query)
            logger.info("📋 Pre-caching existing users...")
            existing_users = {}
            for user in User.query.all():
                if user.username:
                    existing_users[user.username.lower()] = user
            logger.info(f"💾 Cached {len(existing_users)} existing users")

            # 2. STEP 2: Get unique groups from active permissions
            logger.info("📋 Getting unique groups from active permissions...")
            unique_groups = set()
            active_folders = Folder.query.filter_by(is_active=True).all()

            for folder in active_folders:
                for permission in folder.permissions:
                    if permission.is_active:
                        unique_groups.add(permission.ad_group.distinguished_name)

            unique_groups_list = list(unique_groups)
            logger.info(f"📦 Found {len(unique_groups_list)} unique groups to process")

            # Update task state
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 0,
                    'total': len(unique_groups_list),
                    'message': 'Obteniendo miembros de grupos...'
                }
            )

            # 3. STEP 3: Get all group memberships in batches (Optimized LDAP queries)
            ldap_service = LDAPService()
            conn = ldap_service.get_connection()
            if not conn:
                raise Exception('No se pudo conectar a LDAP')

            all_group_memberships = {}
            processed_groups = 0

            # Process groups in batches to avoid memory issues
            for i in range(0, len(unique_groups_list), batch_size_groups):
                batch = unique_groups_list[i:i + batch_size_groups]
                logger.info(f"📦 Processing group batch {i//batch_size_groups + 1}: {len(batch)} groups")

                for group_dn in batch:
                    try:
                        members = ldap_service.get_group_members(group_dn)
                        # Extract usernames from DNs
                        usernames = []
                        for member_dn in members:
                            username = _extract_username_from_dn(member_dn)
                            if username:
                                usernames.append(username.lower())

                        all_group_memberships[group_dn] = usernames
                        logger.debug(f"Group {group_dn}: {len(usernames)} members")

                    except Exception as group_error:
                        logger.error(f"❌ Error getting members for group {group_dn}: {str(group_error)}")
                        all_group_memberships[group_dn] = []

                processed_groups += len(batch)
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'current': processed_groups,
                        'total': len(unique_groups_list),
                        'message': f'Procesando grupos {processed_groups}/{len(unique_groups_list)}...'
                    }
                )

            conn.unbind()

            # 4. STEP 4: Process memberships with intelligent fallback
            logger.info("🔍 Processing memberships with fallback...")

            stats = {
                'success': True,
                'groups_processed': len(unique_groups_list),
                'memberships_processed': 0,
                'users_found_in_cache': 0,
                'users_looked_up_in_ad': 0,
                'users_created_on_demand': 0,
                'users_not_found_in_ad': 0,
                'errors': [],
                'task_id': self.request.id
            }

            failed_user_lookups = set()  # Cache for failed lookups
            fallback_lookups_count = 0

            # Update task state
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 0,
                    'total': len(all_group_memberships),
                    'message': 'Sincronizando membresías...'
                }
            )

            processed_groups_count = 0
            batch_operations = 0

            for group_dn, usernames in all_group_memberships.items():
                try:
                    # Get AD group object
                    ad_group = ADGroup.query.filter_by(distinguished_name=group_dn).first()
                    if not ad_group:
                        logger.warning(f"AD Group not found in database: {group_dn}")
                        continue

                    logger.debug(f"Processing {len(usernames)} members for group {ad_group.name}")

                    for username in usernames:
                        try:
                            # STEP 4A: Check cache first (99% of cases)
                            if username in existing_users:
                                user = existing_users[username]

                                # If user has problematic status, verify in AD before changing status
                                if user.ad_status in ['not_found', 'error', 'disabled']:
                                    logger.info(f"🔍 User {username} has problematic status '{user.ad_status}', verifying in AD...")
                                    try:
                                        user_details = ldap_service.get_user_details(username)
                                        if user_details:
                                            # Check if user is disabled in AD
                                            if user_details.get('is_disabled', False):
                                                user.mark_ad_disabled()
                                                logger.info(f"🔒 User {username} verified as DISABLED in AD")
                                            else:
                                                # Verified as active - update status
                                                user.mark_ad_active()
                                                logger.info(f"✅ User {username} verified and reactivated")
                                        else:
                                            # Still not found - skip
                                            logger.warning(f"❌ User {username} in group but not found in AD verification")
                                            continue
                                    except Exception as verify_error:
                                        logger.error(f"❌ Error verifying user {username}: {str(verify_error)}")
                                        continue
                                else:
                                    # User status is OK, just update last_sync timestamp
                                    user.last_sync = datetime.utcnow()

                                stats['users_found_in_cache'] += 1
                            else:
                                # STEP 4B: User not in cache - fallback lookup
                                if not enable_fallback:
                                    logger.debug(f"👻 Skipping non-existent user: {username} (fallback disabled)")
                                    continue

                                # Process all users - no limits

                                if username in failed_user_lookups:
                                    logger.debug(f"👻 Skipping known failed user: {username}")
                                    continue

                                logger.info(f"🔍 User not found in cache: {username}, looking up in AD...")
                                stats['users_looked_up_in_ad'] += 1
                                fallback_lookups_count += 1

                                try:
                                    user_details = ldap_service.get_user_details(username)
                                    if user_details:
                                        # Create user on demand
                                        new_user = User(
                                            username=username,
                                            full_name=user_details.get('full_name', username),
                                            email=user_details.get('email'),
                                            department=user_details.get('department'),
                                            is_active=True,
                                            created_by_id=requesting_user.id
                                        )
                                        db.session.add(new_user)
                                        db.session.flush()  # Get ID

                                        # Mark as active in AD since we just found them
                                        new_user.mark_ad_active()

                                        # Update cache
                                        existing_users[username] = new_user
                                        user = new_user

                                        stats['users_created_on_demand'] += 1
                                        logger.info(f"✅ User created on demand: {username}")
                                    else:
                                        # User not found in AD - mark if exists in DB
                                        existing_user = User.query.filter_by(username=username).first()
                                        if existing_user:
                                            existing_user.mark_ad_not_found()
                                            logger.warning(f"❌ User {username} not found in AD - marked as not_found and inactive")

                                        failed_user_lookups.add(username)
                                        stats['users_not_found_in_ad'] += 1
                                        logger.warning(f"❌ User {username} not found in AD")
                                        continue

                                except Exception as user_lookup_error:
                                    # Mark user as having AD error if exists in DB
                                    existing_user = User.query.filter_by(username=username).first()
                                    if existing_user:
                                        existing_user.mark_ad_error()
                                        logger.warning(f"❌ User {username} AD lookup error - marked with error status")

                                    failed_user_lookups.add(username)
                                    logger.error(f"❌ Error looking up user {username}: {str(user_lookup_error)}")
                                    stats['errors'].append(f"Error buscando usuario {username}: {str(user_lookup_error)}")
                                    continue

                            # STEP 4C: Create or update membership
                            existing_membership = UserADGroupMembership.query.filter_by(
                                user_id=user.id,
                                ad_group_id=ad_group.id
                            ).first()

                            if not existing_membership:
                                membership = UserADGroupMembership(
                                    user_id=user.id,
                                    ad_group_id=ad_group.id,
                                    granted_at=datetime.utcnow(),
                                    granted_by_id=requesting_user.id,
                                    is_active=True
                                )
                                db.session.add(membership)
                                logger.debug(f"✅ Created membership: {username} -> {ad_group.name}")
                            else:
                                # Ensure existing membership is active
                                if not existing_membership.is_active:
                                    existing_membership.is_active = True
                                    existing_membership.granted_at = datetime.utcnow()
                                    logger.debug(f"🔄 Reactivated membership: {username} -> {ad_group.name}")

                            stats['memberships_processed'] += 1
                            batch_operations += 1

                            # Commit in batches
                            if batch_operations % batch_size_commits == 0:
                                try:
                                    db.session.commit()
                                    logger.debug(f"✅ Batch committed: {batch_operations} operations")
                                except Exception as commit_error:
                                    logger.error(f"❌ Batch commit failed: {str(commit_error)}")
                                    db.session.rollback()
                                    stats['errors'].append(f"Error en commit: {str(commit_error)}")

                        except Exception as member_error:
                            logger.error(f"❌ Error processing member {username}: {str(member_error)}")
                            stats['errors'].append(f"Error procesando miembro {username}: {str(member_error)}")
                            continue

                except Exception as group_error:
                    logger.error(f"❌ Error processing group {group_dn}: {str(group_error)}")
                    stats['errors'].append(f"Error procesando grupo {group_dn}: {str(group_error)}")
                    continue

                processed_groups_count += 1
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'current': processed_groups_count,
                        'total': len(all_group_memberships),
                        'message': f'Procesando membresías {processed_groups_count}/{len(all_group_memberships)}...'
                    }
                )

            # Final commit
            try:
                db.session.commit()
                logger.info("✅ Final commit completed")
            except Exception as final_commit_error:
                logger.error(f"❌ Final commit failed: {str(final_commit_error)}")
                db.session.rollback()
                stats['errors'].append(f"Error en commit final: {str(final_commit_error)}")

            # Generate comprehensive summary
            stats['summary'] = {
                'total_groups': len(unique_groups_list),
                'groups_processed': stats['groups_processed'],
                'memberships_processed': stats['memberships_processed'],
                'users_found_in_cache': stats['users_found_in_cache'],
                'cache_hit_rate': f"{(stats['users_found_in_cache'] / max(1, stats['memberships_processed'])) * 100:.1f}%",
                'users_looked_up_in_ad': stats['users_looked_up_in_ad'],
                'users_created_on_demand': stats['users_created_on_demand'],
                'users_not_found_in_ad': stats['users_not_found_in_ad'],
                'failed_user_lookups_cached': len(failed_user_lookups),
                'errors_count': len(stats['errors']),
                'configuration': {
                    'max_fallback_lookups': max_fallback_lookups,
                    'batch_size_groups': batch_size_groups,
                    'batch_size_commits': batch_size_commits,
                    'fallback_enabled': enable_fallback
                },
                'optimizations_applied': {
                    'user_caching': True,
                    'group_batch_processing': True,
                    'intelligent_fallback': True,
                    'failed_lookup_caching': True,
                    'memory_optimization': True,
                    'ldap_query_optimization': True
                }
            }

            logger.info(f"""
🎉 OPTIMIZED membership sync completed:
   • Groups processed: {stats['groups_processed']}
   • Memberships processed: {stats['memberships_processed']}
   • Cache hits: {stats['users_found_in_cache']} ({stats['summary']['cache_hit_rate']})
   • AD lookups: {stats['users_looked_up_in_ad']}
   • Users created on demand: {stats['users_created_on_demand']}
   • Users not found: {stats['users_not_found_in_ad']}
   • Errors: {len(stats['errors'])}
""")

            # Log audit event
            AuditEvent.log_event(
                user=requesting_user,
                event_type='ad_sync',
                action='sync_memberships_optimized_completed',
                resource_type='system',
                description=f'Sincronización optimizada de membresías completada - Task ID: {self.request.id}',
                metadata={
                    'task_id': self.request.id,
                    'groups_processed': stats['groups_processed'],
                    'memberships_processed': stats['memberships_processed'],
                    'cache_hit_rate': stats['summary']['cache_hit_rate'],
                    'users_created_on_demand': stats['users_created_on_demand'],
                    'optimization_enabled': True
                }
            )

            return stats

    except Exception as e:
        logger.error(f"❌ Optimized membership sync task failed: {str(e)}")

        # Log failure audit event
        try:
            with app.app_context():
                requesting_user = User.query.get(user_id)
                if requesting_user:
                    AuditEvent.log_event(
                        user=requesting_user,
                        event_type='ad_sync',
                        action='sync_memberships_optimized_failed',
                        resource_type='system',
                        description=f'Sincronización optimizada de membresías falló - Task ID: {self.request.id}',
                        metadata={
                            'task_id': self.request.id,
                            'error': str(e),
                            'optimization_enabled': True
                        }
                    )
        except:
            pass

        # Update task state with failure
        self.update_state(
            state='FAILURE',
            meta={
                'error': str(e),
                'message': f'Error en sincronización optimizada: {str(e)}'
            }
        )
        raise e

def _extract_username_from_dn(member_dn):
    """
    Extract username from Distinguished Name with robust parsing
    """
    try:
        if not member_dn:
            return None

        member_dn_lower = member_dn.lower()

        # Skip known non-user objects
        if any(skip_pattern in member_dn_lower for skip_pattern in [
            'ou=devices', 'ou=computers', 'cn=protected users',
            'foreignsecurityprincipals', 's-1-5-'
        ]):
            return None

        # Robust CN extraction - handle various DN formats
        if 'cn=' in member_dn_lower:
            cn_parts = member_dn_lower.split('cn=')
            if len(cn_parts) > 1:
                # Get the first CN= part (typically the user)
                cn_value = cn_parts[1].split(',')[0].strip()
                if cn_value and not any(x in cn_value for x in ['users', 'builtin', 'system']):
                    return cn_value
        elif 'uid=' in member_dn_lower:
            uid_parts = member_dn_lower.split('uid=')
            if len(uid_parts) > 1:
                return uid_parts[1].split(',')[0].strip()

        return None

    except (IndexError, AttributeError):
        return None

@celery.task(bind=True, queue='sync_heavy', name='celery_worker.sync_users_from_ad_task')
def sync_users_from_ad_task(self, user_id):
    """
    Background task to sync ALL users with permissions from AD for all active folders
    Uses configuration from environment variables instead of parameters
    """
    try:
        with app.app_context():
            from app.services.ldap_service import LDAPService
            from app.models import Folder, User, UserADGroupMembership, ADGroup, AuditEvent
            from app import db
            from datetime import datetime
            import os
            import logging
            
            logger = logging.getLogger(__name__)
            logger.info(f"🚀 Starting FULL background sync task - Task ID: {self.request.id}")
            
            # Get configuration from environment
            max_folders = int(os.getenv('BACKGROUND_SYNC_MAX_FOLDERS', 50))
            max_members_per_group = int(os.getenv('BACKGROUND_SYNC_MAX_MEMBERS_PER_GROUP', 200))
            batch_size = int(os.getenv('BACKGROUND_SYNC_BATCH_SIZE', 25))
            enable_full_sync = os.getenv('BACKGROUND_SYNC_ENABLE_FULL_SYNC', 'true').lower() == 'true'
            
            logger.info(f"📋 Configuration: max_folders={max_folders}, max_members={max_members_per_group}, batch_size={batch_size}, full_sync={enable_full_sync}")
            
            # Get requesting user
            requesting_user = User.query.get(user_id)
            if not requesting_user:
                raise Exception(f"User with ID {user_id} not found")
            
            # Get total count of active folders
            total_folders_count = Folder.query.filter_by(is_active=True).count()
            logger.info(f"📁 Total active folders in system: {total_folders_count}")
            logger.info(f"📦 Processing folders in batches of {max_folders}")
            
            # Update task state with real total
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 0,
                    'total': total_folders_count,
                    'message': 'Iniciando sincronización completa...'
                }
            )
            
            ldap_service = LDAPService()
            
            results = {
                'success': True,
                'folders_processed': 0,
                'users_synced': 0,
                'memberships_created': 0,
                'memberships_updated': 0,
                'errors': [],
                'summary': {},
                'large_groups_processed': 0,
                'task_id': self.request.id
            }
            
            # Update task progress
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 0,
                    'total': total_folders_count,
                    'message': 'Conectando a LDAP y pre-cargando datos...'
                }
            )
            
            conn = ldap_service.get_connection()
            if not conn:
                raise Exception('No se pudo conectar a LDAP')
            
            # Pre-cache all users to avoid repeated queries
            logger.info("📋 Pre-caching existing users...")
            existing_users = {}
            for user in User.query.all():
                if user.username:
                    existing_users[user.username.lower()] = user
            logger.info(f"💾 Cached {len(existing_users)} existing users")

            # Cache for failed user lookups to avoid repeated LDAP queries
            failed_user_lookups = set()
            logger.info("🚫 Initialized cache for failed user lookups")
            
            # Process folders in batches
            offset = 0
            total_processed = 0
            
            while offset < total_folders_count:
                # Get current batch of folders
                folders_batch = Folder.query.filter_by(is_active=True).offset(offset).limit(max_folders).all()
                
                if not folders_batch:
                    break
                
                logger.info(f"📦 Processing batch {offset//max_folders + 1}: folders {offset + 1} to {min(offset + len(folders_batch), total_folders_count)} of {total_folders_count}")
                
                # Process each folder in the current batch
                for folder_index, folder in enumerate(folders_batch):
                    folder_global_index = offset + folder_index
                    try:
                        folder_users_synced = 0
                        folder_memberships_created = 0
                        folder_memberships_updated = 0
                        
                        # Update task progress
                        self.update_state(
                            state='PROGRESS',
                            meta={
                                'current': folder_global_index + 1,
                                'total': total_folders_count,
                                'message': f'Procesando carpeta: {folder.name} ({folder_global_index + 1}/{total_folders_count}) - Lote {offset//max_folders + 1}'
                            }
                        )
                        
                        logger.info(f"=== Processing folder: {folder.name} ({folder_global_index + 1}/{total_folders_count}) - Batch {offset//max_folders + 1} ===")
                        
                        # Get all active permissions for this folder
                        active_permissions = [fp for fp in folder.permissions if fp.is_active]
                        logger.info(f"Found {len(active_permissions)} active permissions for folder {folder.name}")
                        
                        if not active_permissions:
                            logger.warning(f"No active permissions found for folder {folder.name}")
                            continue
                        
                        # Process each permission (group) for this folder
                        for permission in active_permissions:
                            ad_group = permission.ad_group
                            logger.info(f"Processing group {ad_group.name} for folder {folder.name}")
                            
                            try:
                                # Get group members from AD
                                group_members = ldap_service.get_group_members(ad_group.distinguished_name)
                                logger.info(f"Found {len(group_members)} members in group {ad_group.name}")
                            except Exception as group_error:
                                logger.error(f"❌ Failed to get members for group {ad_group.name}: {str(group_error)}")
                                results['errors'].append(f"Error obteniendo miembros del grupo {ad_group.name}: {str(group_error)}")
                                continue
                            
                            if not group_members:
                                logger.warning(f"No members found for group {ad_group.name}")
                                continue
                            
                            # Log warning for large groups but PROCESS ALL MEMBERS (completeness over speed)
                            if len(group_members) > max_members_per_group:
                                logger.warning(f"⚠️ Processing large group {ad_group.name} with {len(group_members)} members (above recommended limit of {max_members_per_group}, but processing ALL for completeness)")
                                results['large_groups_processed'] += 1
                                # Continue processing - no skip for 100% completion
                        
                            # Process group members in batches
                            processed_members = 0
                            
                            for i, member_dn in enumerate(group_members):
                                try:
                                    logger.debug(f"Processing member DN: {member_dn}")
                                    
                                    # Extract username from DN with robust parsing
                                    sam_account = None
                                    member_dn_lower = member_dn.lower()

                                    # Skip known non-user objects
                                    if any(skip_pattern in member_dn_lower for skip_pattern in [
                                        'ou=devices', 'ou=computers', 'cn=protected users',
                                        'foreignsecurityprincipals', 's-1-5-'
                                    ]):
                                        logger.debug(f"Skipping non-user object: {member_dn}")
                                        continue

                                    try:
                                        # Robust CN extraction - handle various DN formats
                                        if 'cn=' in member_dn_lower:
                                            cn_parts = member_dn_lower.split('cn=')
                                            if len(cn_parts) > 1:
                                                # Get the first CN= part (typically the user)
                                                cn_value = cn_parts[1].split(',')[0].strip()
                                                if cn_value and not any(x in cn_value for x in ['users', 'builtin', 'system']):
                                                    sam_account = cn_value
                                        elif 'uid=' in member_dn_lower:
                                            uid_parts = member_dn_lower.split('uid=')
                                            if len(uid_parts) > 1:
                                                sam_account = uid_parts[1].split(',')[0].strip()
                                    except (IndexError, AttributeError) as parse_error:
                                        logger.warning(f"Error parsing DN {member_dn}: {str(parse_error)}")
                                        continue

                                    if not sam_account or len(sam_account) < 2:
                                        logger.debug(f"Could not extract valid username from DN: {member_dn}")
                                        continue
                                        
                                    username = sam_account.lower()
                                    
                                    # If FULL sync is enabled, create users that don't exist
                                    if enable_full_sync and username not in existing_users:
                                        # Skip if we already know this user doesn't exist in AD
                                        if username in failed_user_lookups:
                                            logger.debug(f"👻 Skipping known failed user: {username} (cached)")
                                            continue

                                        logger.info(f"🔍 Full sync: Looking up new user {username} in AD...")
                                        try:
                                            # Do individual LDAP lookup for new user
                                            user_details = ldap_service.get_user_details(username)
                                            if user_details:
                                                new_user = User(
                                                    username=username,
                                                    full_name=user_details.get('displayName', username),
                                                    email=user_details.get('mail'),
                                                    department=user_details.get('department'),
                                                    is_active=True,
                                                    created_by_id=requesting_user.id
                                                )
                                                db.session.add(new_user)
                                                db.session.flush()  # Get the ID
                                                existing_users[username] = new_user
                                                folder_users_synced += 1
                                                logger.info(f"✅ Created new user: {username}")
                                            else:
                                                logger.warning(f"Could not find user details in AD: {username}")
                                                failed_user_lookups.add(username)  # Cache the failed lookup
                                                continue
                                        except Exception as user_lookup_error:
                                            logger.error(f"❌ Error looking up user {username}: {str(user_lookup_error)}")
                                            failed_user_lookups.add(username)  # Cache the failed lookup
                                            continue
                                    elif username not in existing_users:
                                        # Skip non-existent users if full sync is disabled
                                        logger.debug(f"👻 Skipping non-existent user: {username} (full sync disabled)")
                                        continue

                                    user = existing_users[username]

                                    # If user has problematic status, verify in AD before changing status
                                    if user.ad_status in ['not_found', 'error', 'disabled']:
                                        logger.info(f"🔍 User {username} has problematic status '{user.ad_status}', verifying in AD...")
                                        try:
                                            user_details = ldap_service.get_user_details(username)
                                            if user_details:
                                                # Check if user is disabled in AD
                                                if user_details.get('is_disabled', False):
                                                    user.mark_ad_disabled()
                                                    logger.info(f"🔒 User {username} verified as DISABLED in AD")
                                                else:
                                                    # Verified as active - update status
                                                    user.mark_ad_active()
                                                    logger.info(f"✅ User {username} verified and reactivated")
                                            else:
                                                # Still not found - skip
                                                logger.warning(f"❌ User {username} in group but not found in AD verification")
                                                continue
                                        except Exception as verify_error:
                                            logger.error(f"❌ Error verifying user {username}: {str(verify_error)}")
                                            continue
                                    else:
                                        # User status is OK, just update last_sync timestamp
                                        user.last_sync = datetime.utcnow()
                                    
                                    # Check if membership already exists
                                    existing_membership = UserADGroupMembership.query.filter_by(
                                        user_id=user.id,
                                        ad_group_id=ad_group.id
                                    ).first()
                                    
                                    if not existing_membership:
                                        # Create new membership
                                        membership = UserADGroupMembership(
                                            user_id=user.id,
                                            ad_group_id=ad_group.id,
                                            granted_at=datetime.utcnow(),
                                            granted_by_id=requesting_user.id,
                                            is_active=True
                                        )
                                        
                                        db.session.add(membership)
                                        folder_memberships_created += 1
                                        logger.debug(f"✅ Created membership: {username} -> {ad_group.name}")
                                    else:
                                        # Ensure existing membership is active
                                        if not existing_membership.is_active:
                                            existing_membership.is_active = True
                                            existing_membership.granted_at = datetime.utcnow()
                                            folder_memberships_updated += 1
                                            logger.debug(f"🔄 Reactivated membership: {username} -> {ad_group.name}")
                                    
                                    processed_members += 1
                                    
                                    # Commit in batches
                                    if processed_members % batch_size == 0:
                                        try:
                                            db.session.commit()
                                            logger.debug(f"✅ Batch committed: {processed_members} members processed")
                                        except Exception as commit_error:
                                            logger.error(f"❌ Batch commit failed: {str(commit_error)}")
                                            db.session.rollback()
                                            results['errors'].append(f"Error en commit: {str(commit_error)}")
                                
                                except Exception as member_error:
                                    logger.error(f"❌ Error processing member {member_dn}: {str(member_error)}")
                                    results['errors'].append(f"Error procesando miembro {member_dn}: {str(member_error)}")
                                    continue
                    
                        results['folders_processed'] += 1
                        results['users_synced'] += folder_users_synced
                        results['memberships_created'] += folder_memberships_created
                        results['memberships_updated'] += folder_memberships_updated
                        
                        logger.info(f"📊 Folder {folder.name} completed: {folder_users_synced} users, {folder_memberships_created} new memberships, {folder_memberships_updated} updated")
                        
                    except Exception as e:
                        logger.error(f"Error processing folder {folder.id}: {str(e)}")
                        results['errors'].append(f"Carpeta {folder.name}: {str(e)}")
                        continue
                
                # Update total processed count for this batch
                total_processed += len(folders_batch)
                logger.info(f"📦 Batch {offset//max_folders + 1} completed: processed {len(folders_batch)} folders, total processed: {total_processed}")
                
                # Move to next batch
                offset += max_folders
            
            logger.info(f"🎉 All batches completed: {total_processed} folders processed in total")
            
            # Final commit for any remaining changes
            try:
                db.session.commit()
                logger.info("✅ Final commit completed")
            except Exception as final_commit_error:
                logger.error(f"❌ Final commit failed: {str(final_commit_error)}")
                db.session.rollback()
                results['errors'].append(f"Error en commit final: {str(final_commit_error)}")
            
            conn.unbind()
            
            # Create comprehensive summary
            total_batches_processed = (offset // max_folders) + (1 if offset % max_folders > 0 else 0)
            results['summary'] = {
                'total_folders_in_system': total_folders_count,
                'folders_processed': results['folders_processed'],
                'users_synced': results['users_synced'],
                'memberships_created': results['memberships_created'],
                'memberships_updated': results['memberships_updated'],
                'errors_count': len(results['errors']),
                'large_groups_processed': results['large_groups_processed'],
                'failed_user_lookups_cached': len(failed_user_lookups),
                'batches_processed': total_batches_processed,
                'configuration': {
                    'batch_size_folders': max_folders,
                    'max_members_per_group': max_members_per_group,
                    'batch_size_members': batch_size,
                    'full_sync_enabled': enable_full_sync
                },
                'optimizations_applied': {
                    'background_task': True,
                    'folder_batch_processing': True,
                    'member_batch_processing': True,
                    'user_caching': True,
                    'failed_lookup_caching': True,
                    'full_sync_mode': enable_full_sync,
                    'timeout_prevention': True,
                    'memory_optimization': True,
                    'cpu_optimization': True
                }
            }
            
            logger.info(f"🎉 FULL background sync completed in {total_batches_processed} batches: {results['folders_processed']}/{total_folders_count} folders, {results['users_synced']} users, {results['memberships_created']} new memberships, {results['memberships_updated']} updated, {len(failed_user_lookups)} failed user lookups cached (CPU optimized)")
            
            # Log comprehensive audit event
            AuditEvent.log_event(
                user=requesting_user,
                event_type='ad_sync',
                action='sync_users_from_ad_background_completed',
                resource_type='system',
                description=f'Sincronización completa en background completada - Task ID: {self.request.id}',
                metadata={
                    'task_id': self.request.id,
                    'folders_processed': results['folders_processed'],
                    'users_synced': results['users_synced'],
                    'memberships_created': results['memberships_created'],
                    'memberships_updated': results['memberships_updated'],
                    'errors_count': len(results['errors']),
                    'large_groups_processed': results['large_groups_processed'],
                    'full_sync_mode': enable_full_sync,
                    'configuration': results['summary']['configuration']
                }
            )
            
            return results
            
    except Exception as e:
        logger.error(f"❌ Background sync task failed: {str(e)}")
        
        # Log failure audit event
        try:
            with app.app_context():
                requesting_user = User.query.get(user_id)
                if requesting_user:
                    AuditEvent.log_event(
                        user=requesting_user,
                        event_type='ad_sync',
                        action='sync_users_from_ad_background_failed',
                        resource_type='system',
                        description=f'Sincronización completa en background falló - Task ID: {self.request.id}',
                        metadata={
                            'task_id': self.request.id,
                            'error': str(e),
                            'background_task_failed': True
                        }
                    )
        except:
            pass
        
        # Update task state with failure
        self.update_state(
            state='FAILURE',
            meta={
                'error': str(e),
                'message': f'Error en sincronización completa: {str(e)}'
            }
        )
        raise e

