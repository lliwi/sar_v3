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

            # Sincronización de permisos activos (sync_users_from_ad_old)
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
        """Ejecuta la sincronización completa de usuarios con permisos activos desde AD"""
        try:
            logger.info("Starting automatic active permissions synchronization (sync_users_from_ad_old)")

            # Crear usuario del sistema para el audit log
            system_user = self._get_or_create_system_user()

            # Test LDAP connection first
            conn = self.ldap_service.get_connection()
            if not conn:
                raise Exception("No se pudo conectar a LDAP")
            conn.unbind()

            # Import and execute the sync_users_from_ad_old function logic
            from app.models import Folder, User, FolderPermission, UserADGroupMembership, ADGroup
            import ldap3

            results = {
                'success': True,
                'folders_processed': 0,
                'users_synced': 0,
                'memberships_created': 0,
                'errors': []
            }

            # Get all active folders
            folders = Folder.query.filter_by(is_active=True).all()
            logger.info(f"🚀 AUTOMATIC active permissions sync: {len(folders)} folders")

            ldap_conn = self.ldap_service.get_connection()
            if not ldap_conn:
                raise Exception("No se pudo conectar a LDAP para sincronización activa")

            # Pre-cache existing users to avoid repeated queries
            existing_users = {}
            for user in User.query.all():
                if user.username:
                    existing_users[user.username.lower()] = user
            logger.info(f"💾 Cached {len(existing_users)} existing users")

            for folder in folders:
                try:
                    folder_users_synced = 0
                    folder_memberships_created = 0
                    logger.info(f"=== Processing folder: {folder.name} (ID: {folder.id}) ===")

                    # Get all active permissions for this folder
                    active_permissions = [fp for fp in folder.permissions if fp.is_active]
                    logger.info(f"Found {len(active_permissions)} active permissions for folder {folder.name}")

                    if not active_permissions:
                        logger.warning(f"No active permissions found for folder {folder.name}")
                        results['folders_processed'] += 1
                        continue

                    for permission in active_permissions:
                        ad_group = permission.ad_group
                        logger.info(f"Processing group {ad_group.name} for folder {folder.name}")

                        try:
                            # Get group members from AD
                            group_members = self.ldap_service.get_group_members(ad_group.distinguished_name)
                            logger.info(f"Found {len(group_members)} members in group {ad_group.name}")
                        except Exception as group_error:
                            logger.error(f"❌ Failed to get members for group {ad_group.name}: {str(group_error)}")
                            results['errors'].append(f"Error obteniendo miembros del grupo {ad_group.name}: {str(group_error)}")
                            continue

                        if not group_members:
                            logger.warning(f"No members found for group {ad_group.name}")
                            continue

                        # Process group members (limit to 100 for automatic sync to prevent timeouts)
                        processed_count = 0
                        max_members_auto = 100  # Limit for automatic sync

                        for member_dn in group_members[:max_members_auto]:
                            try:
                                # Skip Foreign Security Principals
                                if 'ForeignSecurityPrincipals' in member_dn or 'S-1-5-' in member_dn:
                                    continue

                                # Get user details from AD
                                sam_account = None
                                full_name = None
                                email = None

                                # Search user by DN
                                search_filter = f"(distinguishedName={member_dn})"
                                attributes = ['sAMAccountName', 'displayName', 'mail', 'cn']

                                ldap_conn.search(
                                    search_base=self.ldap_service.base_dn,
                                    search_filter=search_filter,
                                    attributes=attributes,
                                    search_scope=ldap3.SUBTREE
                                )

                                if ldap_conn.entries:
                                    user_entry = ldap_conn.entries[0]
                                    sam_account = str(user_entry.sAMAccountName) if user_entry.sAMAccountName else None
                                    full_name = str(user_entry.displayName) if user_entry.displayName else str(user_entry.cn) if user_entry.cn else None
                                    email = str(user_entry.mail) if user_entry.mail else None

                                if sam_account:
                                    # Find or create user in database
                                    user = existing_users.get(sam_account.lower())
                                    if not user:
                                        user = User.query.filter_by(username=sam_account.lower()).first()
                                        if not user:
                                            # Create new user
                                            user = User(
                                                username=sam_account.lower(),
                                                email=email or f"{sam_account}@example.org",
                                                full_name=full_name or sam_account,
                                                distinguished_name=member_dn,
                                                is_active=True
                                            )
                                            db.session.add(user)
                                            db.session.flush()
                                            existing_users[sam_account.lower()] = user
                                            folder_users_synced += 1
                                        else:
                                            existing_users[sam_account.lower()] = user

                                    # Check/create membership
                                    existing_membership = UserADGroupMembership.query.filter_by(
                                        user_id=user.id,
                                        ad_group_id=ad_group.id
                                    ).first()

                                    if not existing_membership:
                                        membership = UserADGroupMembership(
                                            user_id=user.id,
                                            ad_group_id=ad_group.id
                                        )
                                        db.session.add(membership)
                                        folder_memberships_created += 1

                                    processed_count += 1

                            except Exception as member_error:
                                logger.error(f"Error processing member {member_dn}: {str(member_error)}")
                                continue

                        logger.info(f"Processed {processed_count}/{len(group_members)} members for group {ad_group.name}")

                    results['folders_processed'] += 1
                    results['users_synced'] += folder_users_synced
                    results['memberships_created'] += folder_memberships_created

                    # Commit changes for this folder
                    db.session.commit()

                except Exception as folder_error:
                    logger.error(f"Error processing folder {folder.name}: {str(folder_error)}")
                    results['errors'].append(f"Error procesando carpeta {folder.name}: {str(folder_error)}")
                    db.session.rollback()
                    continue

            # Close LDAP connection
            ldap_conn.unbind()

            self.last_active_permissions_sync = datetime.utcnow()

            # Log audit event
            AuditEvent.log_event(
                user=system_user,
                event_type='ad_sync',
                action='automatic_sync_active_permissions',
                description=f'Sincronización automática de permisos activos: {results["folders_processed"]} carpetas, {results["users_synced"]} usuarios, {results["memberships_created"]} membresías creadas',
                metadata={
                    'folders_processed': results['folders_processed'],
                    'users_synced': results['users_synced'],
                    'memberships_created': results['memberships_created'],
                    'errors_count': len(results['errors']),
                    'sync_type': 'automatic',
                    'active_permissions_sync_interval': self.get_config()['active_permissions_sync_interval']
                }
            )

            logger.info(f"Automatic active permissions sync completed: {results['folders_processed']} folders, {results['users_synced']} users, {results['memberships_created']} memberships")

        except Exception as e:
            logger.error(f"Error in automatic active permissions sync: {str(e)}")

            # Log error event
            system_user = self._get_or_create_system_user()
            AuditEvent.log_event(
                user=system_user,
                event_type='ad_sync',
                action='automatic_sync_active_permissions_error',
                description=f'Error en sincronización automática de permisos activos: {str(e)}',
                metadata={
                    'error': str(e),
                    'sync_type': 'automatic'
                }
            )

    def _get_or_create_system_user(self) -> User:
        """Obtiene o crea el usuario del sistema para audit logs"""
        system_user = User.query.filter_by(username='system').first()
        if not system_user:
            system_user = User(
                username='system',
                email='system@localhost',
                full_name='Sistema Automático',
                is_active=True
            )
            # Asignar rol de administrador si existe
            from app.models import Role
            admin_role = Role.query.filter_by(name='admin').first()
            if admin_role:
                system_user.roles.append(admin_role)
            
            db.session.add(system_user)
            db.session.commit()
        
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
    
    def get_status(self) -> Dict[str, Any]:
        """Obtiene el estado actual del programador"""
        config = self.get_config()
        
        return {
            'running': self.running,
            'config': config,
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
    
    def _get_next_sync_time(self, sync_type: str, interval_seconds: int) -> Optional[str]:
        """Calcula el próximo tiempo de sincronización"""
        last_sync_attr = f'last_{sync_type}_sync'
        last_sync = getattr(self, last_sync_attr, None)
        
        if last_sync is None:
            return "Inmediatamente"
        
        next_sync = last_sync + timedelta(seconds=interval_seconds)
        return next_sync.isoformat()


# Instancia global del programador
scheduler_service = SchedulerService()