#!/usr/bin/env python3

from app.celery_app import make_celery
from app import create_app
from app.services.email_service import send_permission_request_notification as _send_permission_request_notification
from app.services.email_service import send_permission_status_notification as _send_permission_status_notification

# Create Flask app and configure Celery
app = create_app()
celery = make_celery(app)

# Register email tasks
@celery.task
def send_permission_request_notification(request_id):
    """Celery task wrapper for sending permission request notification email"""
    return _send_permission_request_notification(request_id)

@celery.task
def send_permission_status_notification(request_id, status):
    """Celery task wrapper for sending permission status change notification"""
    return _send_permission_status_notification(request_id, status)

@celery.task(bind=True)
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
            
            # Update task state
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 0,
                    'total': max_folders,
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
                'skipped_large_groups': 0,
                'task_id': self.request.id
            }
            
            # Get requesting user
            requesting_user = User.query.get(user_id)
            if not requesting_user:
                raise Exception(f"User with ID {user_id} not found")
            
            # Get ALL active folders (or limited by config)
            folders = Folder.query.filter_by(is_active=True).limit(max_folders).all()
            logger.info(f"📁 Processing {len(folders)} active folders (limit: {max_folders})")
            
            # Update task progress
            self.update_state(
                state='PROGRESS',
                meta={
                    'current': 1,
                    'total': len(folders),
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
            
            # Process each folder
            for folder_index, folder in enumerate(folders):
                try:
                    folder_users_synced = 0
                    folder_memberships_created = 0
                    folder_memberships_updated = 0
                    
                    # Update task progress
                    self.update_state(
                        state='PROGRESS',
                        meta={
                            'current': folder_index + 1,
                            'total': len(folders),
                            'message': f'Procesando carpeta: {folder.name} ({folder_index + 1}/{len(folders)})'
                        }
                    )
                    
                    logger.info(f"=== Processing folder: {folder.name} ({folder_index + 1}/{len(folders)}) ===")
                    
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
                        
                        # Skip extremely large groups to prevent memory issues
                        if len(group_members) > max_members_per_group:
                            logger.warning(f"⚠️ Skipping large group {ad_group.name} with {len(group_members)} members (limit: {max_members_per_group})")
                            results['skipped_large_groups'] += 1
                            continue
                        
                        # Process group members in batches
                        processed_members = 0
                        
                        for i, member_dn in enumerate(group_members):
                            try:
                                logger.debug(f"Processing member DN: {member_dn}")
                                
                                # Extract username from DN (optimization to avoid individual LDAP queries)
                                sam_account = None
                                member_dn_lower = member_dn.lower()
                                
                                if 'cn=' in member_dn_lower:
                                    sam_account = member_dn.split('cn=')[1].split(',')[0].strip()
                                elif 'uid=' in member_dn_lower:
                                    sam_account = member_dn.split('uid=')[1].split(',')[0].strip()
                                
                                if not sam_account:
                                    logger.warning(f"Could not extract username from DN: {member_dn}")
                                    continue
                                    
                                username = sam_account.lower()
                                
                                # If FULL sync is enabled, create users that don't exist
                                if enable_full_sync and username not in existing_users:
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
                                            continue
                                    except Exception as user_lookup_error:
                                        logger.error(f"❌ Error looking up user {username}: {str(user_lookup_error)}")
                                        continue
                                elif username not in existing_users:
                                    # Skip non-existent users if full sync is disabled
                                    logger.debug(f"👻 Skipping non-existent user: {username} (full sync disabled)")
                                    continue
                                
                                user = existing_users[username]
                                
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
            results['summary'] = {
                'total_folders_in_system': Folder.query.filter_by(is_active=True).count(),
                'folders_processed': results['folders_processed'],
                'users_synced': results['users_synced'],
                'memberships_created': results['memberships_created'],
                'memberships_updated': results['memberships_updated'],
                'errors_count': len(results['errors']),
                'skipped_large_groups': results['skipped_large_groups'],
                'configuration': {
                    'max_folders': max_folders,
                    'max_members_per_group': max_members_per_group,
                    'batch_size': batch_size,
                    'full_sync_enabled': enable_full_sync
                },
                'optimizations_applied': {
                    'background_task': True,
                    'batch_processing': True,
                    'user_caching': True,
                    'full_sync_mode': enable_full_sync,
                    'timeout_prevention': True
                }
            }
            
            logger.info(f"🎉 FULL background sync completed: {results['folders_processed']} folders, {results['users_synced']} users, {results['memberships_created']} new memberships, {results['memberships_updated']} updated, {results['skipped_large_groups']} large groups skipped")
            
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
                    'skipped_large_groups': results['skipped_large_groups'],
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

