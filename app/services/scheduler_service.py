"""
Servicio de programación de tareas para sincronizaciones periódicas de AD.
Este servicio gestiona las tareas automáticas de sincronización de usuarios y grupos AD.
"""

import logging
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from flask import current_app
from app import db
from app.models import AuditEvent, Task, User
from app.services.ldap_service import LDAPService
from app.utils.db_utils import commit_with_retry

logger = logging.getLogger(__name__)


class SchedulerService:
    """Servicio para programar y ejecutar tareas periódicas de sincronización"""

    def __init__(self):
        self.ldap_service = None  # Initialize within app context
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.last_user_sync: Optional[datetime] = None
        self.last_group_sync: Optional[datetime] = None
        self.last_user_permissions_sync: Optional[datetime] = None
        self.last_active_permissions_sync: Optional[datetime] = None
        self._sync_lock = threading.Lock()
        self._instance_id = id(self)

    def get_config(self) -> Dict[str, Any]:
        """Obtiene la configuración del programador desde variables de entorno"""
        return {
            # Intervalos de sincronización específicos (en segundos)
            'user_sync_interval': int(os.getenv('AD_USER_SYNC_INTERVAL', 600)),  # 10 minutos por defecto
            'group_sync_interval': int(os.getenv('AD_GROUP_SYNC_INTERVAL', 300)),  # 5 minutos por defecto
            'user_permissions_sync_interval': int(os.getenv('AD_USER_PERMISSIONS_SYNC_INTERVAL', 900)),  # 15 minutos por defecto
            'active_permissions_sync_interval': int(os.getenv('AD_ACTIVE_PERMISSIONS_SYNC_INTERVAL', 1800)),  # 30 minutos por defecto

            # Intervalo general para compatibilidad (usado por task scheduler)
            'processing_interval': int(os.getenv('TASK_PROCESSING_INTERVAL', 300)),  # 5 minutos por defecto

            # Configuraciones de habilitación
            'user_sync_enabled': os.getenv('AD_USER_SYNC_ENABLED', 'true').lower() == 'true',
            'group_sync_enabled': os.getenv('AD_GROUP_SYNC_ENABLED', 'true').lower() == 'true',
            'user_permissions_sync_enabled': os.getenv('AD_USER_PERMISSIONS_SYNC_ENABLED', 'true').lower() == 'true',
            'active_permissions_sync_enabled': os.getenv('AD_ACTIVE_PERMISSIONS_SYNC_ENABLED', 'true').lower() == 'true',

            # Configuraciones de reintentos
            'max_retries': int(os.getenv('SYNC_MAX_RETRIES', 3)),
            'retry_delay': int(os.getenv('SYNC_RETRY_DELAY', 60))  # 1 minuto por defecto
        }

    def start(self, app):
        """Inicia el servicio de programación en un hilo separado"""
        if self.running:
            logger.warning("Scheduler service is already running")
            return

        self.app = app

        # Initialize LDAP service within app context
        with app.app_context():
            from app.services.ldap_service import LDAPService
            self.ldap_service = LDAPService()

        self.running = True
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
        logger.info("Scheduler service started")

    def stop(self):
        """Detiene el servicio de programación"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)
        logger.info("Scheduler service stopped")

    def _run_scheduler(self):
        """Ejecuta el bucle principal del programador"""
        config = self.get_config()
        interval = config['processing_interval']

        logger.info(f"Starting scheduler loop with interval: {interval} seconds")

        while self.running:
            try:
                with self.app.app_context():
                    self._check_and_run_syncs()

                # Esperar el intervalo especificado
                time.sleep(interval)

            except Exception as e:
                logger.error(f"Error in scheduler loop: {str(e)}")
                time.sleep(60)  # Esperar 1 minuto antes de reintentar

    def _check_and_run_syncs(self):
        """Verifica y ejecuta las sincronizaciones necesarias"""
        # Usar lock para evitar ejecuciones concurrentes
        if not self._sync_lock.acquire(blocking=False):
            logger.debug(f"Sync already running in instance {self._instance_id}, skipping")
            return

        try:
            config = self.get_config()
            now = datetime.utcnow()

            # Sincronización de usuarios
            if config['user_sync_enabled']:
                user_interval_minutes = config['user_sync_interval'] / 60
                if self._should_sync('user', now, user_interval_minutes):
                    self._sync_users()

            # Sincronización de grupos AD
            if config['group_sync_enabled']:
                group_interval_minutes = config['group_sync_interval'] / 60
                if self._should_sync('group', now, group_interval_minutes):
                    self._sync_ad_groups()

            # Sincronización de permisos de usuarios
            if config['user_permissions_sync_enabled']:
                permissions_interval_minutes = config['user_permissions_sync_interval'] / 60
                if self._should_sync('user_permissions', now, permissions_interval_minutes):
                    self._sync_user_permissions()

            # Sincronización de permisos activos (optimized)
            if config['active_permissions_sync_enabled']:
                active_permissions_interval_minutes = config['active_permissions_sync_interval'] / 60
                if self._should_sync('active_permissions', now, active_permissions_interval_minutes):
                    self._sync_active_permissions()
        finally:
            self._sync_lock.release()

    def _should_sync(self, sync_type: str, now: datetime, interval_minutes: float) -> bool:
        """Determina si debe ejecutarse una sincronización específica"""
        last_sync_attr = f'last_{sync_type}_sync'
        last_sync = getattr(self, last_sync_attr, None)

        if last_sync is None:
            return True  # Primera ejecución

        time_since_last = (now - last_sync).total_seconds() / 60
        return time_since_last >= interval_minutes

    def _sync_users(self):
        """Ejecuta la sincronización de usuarios"""
        try:
            logger.info("Starting automatic user synchronization")

            # Crear usuario del sistema para el audit log
            system_user = self._get_or_create_system_user()

            # Test LDAP connection first
            conn = self.ldap_service.get_connection()
            if not conn:
                raise Exception("No se pudo conectar a LDAP")
            conn.unbind()

            synced_count = self.ldap_service.sync_users()
            self.last_user_sync = datetime.utcnow()

            # Log audit event
            AuditEvent.log_event(
                user=system_user,
                event_type='user_sync',
                action='automatic_sync_users',
                description=f'Sincronización automática de usuarios AD: {synced_count} usuarios procesados',
                metadata={
                    'synced_count': synced_count,
                    'sync_type': 'automatic',
                    'user_sync_interval': self.get_config()['user_sync_interval']
                }
            )

            logger.info(f"Automatic user sync completed: {synced_count} users processed")

        except Exception as e:
            logger.error(f"Error in automatic user sync: {str(e)}")

            # Log error event
            system_user = self._get_or_create_system_user()
            AuditEvent.log_event(
                user=system_user,
                event_type='user_sync',
                action='automatic_sync_users_error',
                description=f'Error en sincronización automática de usuarios: {str(e)}',
                metadata={
                    'error': str(e),
                    'sync_type': 'automatic'
                }
            )

    def _sync_ad_groups(self):
        """Ejecuta la sincronización de grupos AD"""
        try:
            logger.info("Starting automatic AD groups synchronization")

            # Crear usuario del sistema para el audit log
            system_user = self._get_or_create_system_user()

            # Test LDAP connection first
            conn = self.ldap_service.get_connection()
            if not conn:
                raise Exception("No se pudo conectar a LDAP")
            conn.unbind()

            synced_count = self.ldap_service.sync_groups()
            self.last_group_sync = datetime.utcnow()

            # Log audit event
            AuditEvent.log_event(
                user=system_user,
                event_type='ad_sync',
                action='automatic_sync_groups',
                description=f'Sincronización automática de grupos AD: {synced_count} grupos procesados',
                metadata={
                    'synced_count': synced_count,
                    'sync_type': 'automatic',
                    'group_sync_interval': self.get_config()['group_sync_interval']
                }
            )

            logger.info(f"Automatic AD groups sync completed: {synced_count} groups processed")

        except Exception as e:
            logger.error(f"Error in automatic AD groups sync: {str(e)}")

            # Log error event
            system_user = self._get_or_create_system_user()
            AuditEvent.log_event(
                user=system_user,
                event_type='ad_sync',
                action='automatic_sync_groups_error',
                description=f'Error en sincronización automática de grupos AD: {str(e)}',
                metadata={
                    'error': str(e),
                    'sync_type': 'automatic'
                }
            )

    def _sync_user_permissions(self):
        """Ejecuta la sincronización de permisos de usuarios desde AD"""
        try:
            logger.info("Starting automatic user permissions synchronization")

            # Removed business hours restriction - sync runs any time for maximum data freshness

            # Crear usuario del sistema para el audit log
            system_user = self._get_or_create_system_user()

            # Test LDAP connection first
            conn = self.ldap_service.get_connection()
            if not conn:
                raise Exception("No se pudo conectar a LDAP")
            conn.unbind()

            # Ejecutar sincronización de usuarios y permisos existentes
            from app.models import Folder, User, FolderPermission, UserADGroupMembership, ADGroup

            results = {
                'folders_processed': 0,
                'users_synced': 0,
                'permissions_processed': 0,
                'errors': []
            }

            # Solo sincronizar usuarios y grupos existentes sin crear nuevos permisos
            # Esta tarea se enfoca en mantener la consistencia de datos ya existentes
            users_synced = self.ldap_service.sync_users()
            groups_synced = self.ldap_service.sync_groups()

            results['users_synced'] = users_synced
            results['permissions_processed'] = groups_synced

            self.last_user_permissions_sync = datetime.utcnow()

            # Log audit event
            AuditEvent.log_event(
                user=system_user,
                event_type='ad_sync',
                action='automatic_sync_user_permissions',
                description=f'Sincronización automática de permisos existentes: {results["users_synced"]} usuarios y {results["permissions_processed"]} grupos',
                metadata={
                    'users_synced': results['users_synced'],
                    'permissions_processed': results['permissions_processed'],
                    'errors_count': len(results['errors']),
                    'sync_type': 'automatic',
                    'user_permissions_sync_interval': self.get_config()['user_permissions_sync_interval']
                }
            )

            logger.info(f"Automatic user permissions sync completed: {results['users_synced']} users and {results['permissions_processed']} groups synchronized")

        except Exception as e:
            logger.error(f"Error in automatic user permissions sync: {str(e)}")

            # Log error event
            system_user = self._get_or_create_system_user()
            AuditEvent.log_event(
                user=system_user,
                event_type='ad_sync',
                action='automatic_sync_user_permissions_error',
                description=f'Error en sincronización automática de permisos existentes: {str(e)}',
                metadata={
                    'error': str(e),
                    'sync_type': 'automatic'
                }
            )

    def _sync_active_permissions(self):
        """Ejecuta la sincronización optimizada de membresías desde AD usando Celery"""
        try:
            logger.info("Starting automatic optimized membership synchronization via Celery task")

            # Crear usuario del sistema para el audit log
            system_user = self._get_or_create_system_user()

            # Test LDAP connection first
            conn = self.ldap_service.get_connection()
            if not conn:
                raise Exception("No se pudo conectar a LDAP")
            conn.unbind()

            # Use optimized Celery task
            try:
                from celery_worker import sync_memberships_optimized_task

                # Launch optimized membership sync task
                task_result = sync_memberships_optimized_task.delay(system_user.id)

                logger.info(f"✅ Automatic optimized membership sync launched as Celery task: {task_result.id}")

                # Update last sync time
                self.last_active_permissions_sync = datetime.utcnow()

                # Log audit event for task launch
                AuditEvent.log_event(
                    user=system_user,
                    event_type='ad_sync',
                    action='automatic_sync_memberships_optimized_started',
                    description=f'Sincronización automática optimizada de membresías iniciada - Task ID: {task_result.id}',
                    metadata={
                        'task_id': task_result.id,
                        'sync_type': 'automatic_optimized_memberships',
                        'celery_queue': 'sync_heavy',
                        'optimized_processing': True,
                        'intelligent_fallback': True
                    }
                )

                return {
                    'success': True,
                    'task_id': task_result.id,
                    'message': 'Sincronización optimizada de membresías iniciada en background',
                    'optimized_processing': True
                }

            except (ImportError, Exception) as e:
                logger.warning(f"Optimized Celery task failed ({str(e)}), falling back to optimized sequential sync")
                # Fallback to optimized sequential processing if Celery not available or fails
                return self._sync_memberships_sequential_optimized()

        except Exception as e:
            logger.error(f"Error in automatic optimized membership sync: {str(e)}")

            # Log error event
            system_user = self._get_or_create_system_user()
            AuditEvent.log_event(
                user=system_user,
                event_type='ad_sync',
                action='automatic_sync_memberships_optimized_error',
                description=f'Error en sincronización automática optimizada: {str(e)}',
                metadata={
                    'error': str(e),
                    'sync_type': 'automatic'
                }
            )

    def _sync_memberships_sequential_optimized(self):
        """
        Optimized sequential membership sync (fallback)
        """
        try:
            # Import required models at the beginning
            from app.models import User, UserADGroupMembership, ADGroup, AuditEvent
            from app import db
            from datetime import datetime

            results = {
                'success': True,
                'groups_processed': 0,
                'memberships_processed': 0,
                'users_found_in_cache': 0,
                'users_looked_up_in_ad': 0,
                'users_created_on_demand': 0,
                'users_not_found_in_ad': 0,
                'errors': []
            }

            logger.info("🚀 Starting OPTIMIZED sequential membership sync (fallback)")

            # 1. Cache all existing users (1 DB query)
            existing_users = {}
            for user in User.query.all():
                if user.username:
                    existing_users[user.username.lower()] = user
            logger.info(f"💾 Cached {len(existing_users)} existing users")

            # 2. Get unique groups from active permissions
            unique_groups = self.ldap_service.get_unique_groups_from_active_permissions()
            logger.info(f"📦 Found {len(unique_groups)} unique groups to process")

            if not unique_groups:
                logger.warning("No unique groups found for processing")
                self.last_active_permissions_sync = datetime.utcnow()
                return results

            # 3. Get all group memberships in batches
            all_group_memberships = self.ldap_service.get_multiple_groups_members_batch(unique_groups, batch_size=5)

            # 4. Process memberships with intelligent fallback
            failed_user_lookups = set()
            max_fallback_lookups = float('inf')  # No limit - process all data

            fallback_lookups_count = 0
            batch_operations = 0
            batch_size_commits = 25

            # Get system user for created_by
            system_user = self._get_or_create_system_user()

            for group_dn, member_dns in all_group_memberships.items():
                try:
                    # Get AD group object
                    ad_group = ADGroup.query.filter_by(distinguished_name=group_dn).first()
                    if not ad_group:
                        logger.warning(f"AD Group not found in database: {group_dn}")
                        continue

                    # Extract usernames from DNs
                    usernames = []
                    for member_dn in member_dns:
                        username = self.ldap_service.extract_username_from_dn(member_dn)
                        if username:
                            usernames.append(username.lower())

                    logger.debug(f"Processing {len(usernames)} members for group {ad_group.name}")

                    for username in usernames:
                        try:
                            # Check cache first (99% of cases)
                            if username in existing_users:
                                user = existing_users[username]

                                # If user has problematic status, verify in AD before changing status
                                if user.ad_status in ['not_found', 'error', 'disabled']:
                                    logger.info(f"🔍 User {username} has problematic status '{user.ad_status}', verifying in AD...")
                                    try:
                                        user_details = self.ldap_service.get_user_details(username)
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

                                results['users_found_in_cache'] += 1
                            else:
                                # User not in cache - fallback lookup
                                # Process all users - no limits

                                if username in failed_user_lookups:
                                    logger.debug(f"👻 Skipping known failed user: {username}")
                                    continue

                                logger.info(f"🔍 User not found in cache: {username}, looking up in AD...")
                                results['users_looked_up_in_ad'] += 1
                                fallback_lookups_count += 1

                                try:
                                    user_details = self.ldap_service.get_user_details(username)
                                    if user_details:
                                        # Create user on demand
                                        new_user = User(
                                            username=username,
                                            full_name=user_details.get('full_name', username),
                                            email=user_details.get('email'),
                                            department=user_details.get('department'),
                                            is_active=True,
                                            created_by_id=system_user.id if system_user else None
                                        )
                                        db.session.add(new_user)
                                        db.session.flush()  # Get ID

                                        # Mark as active in AD since we just found them
                                        new_user.mark_ad_active()

                                        # Update cache
                                        existing_users[username] = new_user
                                        user = new_user

                                        results['users_created_on_demand'] += 1
                                        logger.info(f"✅ User created on demand: {username}")
                                    else:
                                        # User not found in AD - mark if exists in DB
                                        existing_user = User.query.filter_by(username=username).first()
                                        if existing_user:
                                            existing_user.mark_ad_not_found()
                                            logger.warning(f"❌ User {username} not found in AD - marked as not_found and inactive")

                                        failed_user_lookups.add(username)
                                        results['users_not_found_in_ad'] += 1
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
                                    results['errors'].append(f"Error buscando usuario {username}: {str(user_lookup_error)}")
                                    continue

                            # Create or update membership
                            existing_membership = UserADGroupMembership.query.filter_by(
                                user_id=user.id,
                                ad_group_id=ad_group.id
                            ).first()

                            if not existing_membership:
                                membership = UserADGroupMembership(
                                    user_id=user.id,
                                    ad_group_id=ad_group.id,
                                    granted_at=datetime.utcnow(),
                                    granted_by_id=system_user.id if system_user else None,
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

                            results['memberships_processed'] += 1
                            batch_operations += 1

                            # Commit in batches
                            if batch_operations % batch_size_commits == 0:
                                if commit_with_retry(max_attempts=3):
                                    logger.debug(f"✅ Batch committed: {batch_operations} operations")
                                else:
                                    logger.error(f"❌ Batch commit failed after retries")
                                    results['errors'].append(f"Error en commit batch después de reintentos")

                        except Exception as member_error:
                            logger.error(f"❌ Error processing member {username}: {str(member_error)}")
                            results['errors'].append(f"Error procesando miembro {username}: {str(member_error)}")
                            continue

                    results['groups_processed'] += 1

                except Exception as group_error:
                    logger.error(f"❌ Error processing group {group_dn}: {str(group_error)}")
                    results['errors'].append(f"Error procesando grupo {group_dn}: {str(group_error)}")
                    continue

            # Final commit
            if commit_with_retry(max_attempts=3):
                logger.info("✅ Final commit completed")
            else:
                logger.error(f"❌ Final commit failed after retries")
                results['errors'].append(f"Error en commit final después de reintentos")

            # Update last sync time
            self.last_active_permissions_sync = datetime.utcnow()

            # Generate summary
            cache_hit_rate = f"{(results['users_found_in_cache'] / max(1, results['memberships_processed'])) * 100:.1f}%"

            logger.info(f"""
🎉 OPTIMIZED sequential membership sync completed:
   • Groups processed: {results['groups_processed']}
   • Memberships processed: {results['memberships_processed']}
   • Cache hits: {results['users_found_in_cache']} ({cache_hit_rate})
   • AD lookups: {results['users_looked_up_in_ad']}
   • Users created on demand: {results['users_created_on_demand']}
   • Users not found: {results['users_not_found_in_ad']}
   • Errors: {len(results['errors'])}
""")

            # Log audit event
            if system_user:
                AuditEvent.log_event(
                    user=system_user,
                    event_type='ad_sync',
                    action='sync_memberships_sequential_optimized_completed',
                    description='Sincronización secuencial optimizada de membresías completada',
                    metadata={
                        'groups_processed': results['groups_processed'],
                        'memberships_processed': results['memberships_processed'],
                        'cache_hit_rate': cache_hit_rate,
                        'users_created_on_demand': results['users_created_on_demand'],
                        'fallback_mode': 'sequential_optimized'
                    }
                )

            return results

        except Exception as e:
            logger.error(f"❌ Optimized sequential membership sync failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'groups_processed': 0,
                'memberships_processed': 0,
                'errors': [str(e)]
            }

    def _get_or_create_system_user(self):
        """Get or create system user for automatic operations"""
        system_user = User.query.filter_by(username='system').first()
        if not system_user:
            logger.info("Creating system user for automatic operations")
            system_user = User(
                username='system',
                email='system@example.org',
                full_name='Sistema Automático',
                is_active=True
            )
            db.session.add(system_user)
            commit_with_retry(max_attempts=3)

        return system_user

    def force_sync_all(self):
        """Fuerza la sincronización de todos los tipos inmediatamente"""
        try:
            logger.info("Starting forced synchronization of all types")

            with self.app.app_context():
                self._sync_users()
                self._sync_ad_groups()
                self._sync_user_permissions()
                self._sync_active_permissions()

            logger.info("Forced synchronization completed")

        except Exception as e:
            logger.error(f"Error in forced sync: {str(e)}")

    def _get_next_sync_time(self, sync_type: str, interval_seconds: int) -> Optional[str]:
        """Calcula la próxima hora de sincronización para un tipo específico"""
        last_sync_attr = f'last_{sync_type}_sync'
        last_sync = getattr(self, last_sync_attr, None)

        if last_sync is None:
            return "Inmediatamente (primera ejecución)"

        next_sync = last_sync + timedelta(seconds=interval_seconds)
        now = datetime.utcnow()

        if next_sync <= now:
            return "Inmediatamente"
        else:
            time_until = next_sync - now
            hours, remainder = divmod(time_until.total_seconds(), 3600)
            minutes, _ = divmod(remainder, 60)
            return f"En {int(hours)}h {int(minutes)}m"

    def get_status(self) -> Dict[str, Any]:
        """Obtiene el estado actual del programador"""
        config = self.get_config()

        return {
            'running': self.running,
            'instance_id': self._instance_id,
            'configuration': config,
            'last_syncs': {
                'users': self.last_user_sync.isoformat() if self.last_user_sync else None,
                'groups': self.last_group_sync.isoformat() if self.last_group_sync else None,
                'user_permissions': self.last_user_permissions_sync.isoformat() if self.last_user_permissions_sync else None,
                'active_permissions': self.last_active_permissions_sync.isoformat() if self.last_active_permissions_sync else None
            },
            'next_syncs': {
                'users': self._get_next_sync_time('user', config['user_sync_interval']),
                'groups': self._get_next_sync_time('group', config['group_sync_interval']),
                'user_permissions': self._get_next_sync_time('user_permissions', config['user_permissions_sync_interval']),
                'active_permissions': self._get_next_sync_time('active_permissions', config['active_permissions_sync_interval'])
            }
        }