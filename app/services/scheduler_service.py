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
        self._sync_lock = threading.Lock()
        self._instance_id = id(self)
        
    def get_config(self) -> Dict[str, Any]:
        """Obtiene la configuración del programador desde variables de entorno"""
        return {
            # Intervalos de sincronización específicos (en segundos)
            'user_sync_interval': int(os.getenv('AD_USER_SYNC_INTERVAL', 600)),  # 10 minutos por defecto
            'group_sync_interval': int(os.getenv('AD_GROUP_SYNC_INTERVAL', 300)),  # 5 minutos por defecto
            'user_permissions_sync_interval': int(os.getenv('AD_USER_PERMISSIONS_SYNC_INTERVAL', 900)),  # 15 minutos por defecto
            
            # Intervalo general para compatibilidad (usado por task scheduler)
            'processing_interval': int(os.getenv('TASK_PROCESSING_INTERVAL', 300)),  # 5 minutos por defecto
            
            # Configuraciones de habilitación
            'user_sync_enabled': os.getenv('AD_USER_SYNC_ENABLED', 'true').lower() == 'true',
            'group_sync_enabled': os.getenv('AD_GROUP_SYNC_ENABLED', 'true').lower() == 'true',
            'user_permissions_sync_enabled': os.getenv('AD_USER_PERMISSIONS_SYNC_ENABLED', 'true').lower() == 'true',
            
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
            
            # Check if current time is during business hours to avoid peak load
            current_hour = datetime.now().hour
            if 8 <= current_hour <= 18:  # Business hours 8 AM to 6 PM
                logger.info("Skipping sync during business hours to avoid performance impact")
                return
            
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
                'user_permissions': self.last_user_permissions_sync.isoformat() if self.last_user_permissions_sync else None
            },
            'next_syncs': {
                'users': self._get_next_sync_time('user', config['user_sync_interval']),
                'groups': self._get_next_sync_time('group', config['group_sync_interval']),
                'user_permissions': self._get_next_sync_time('user_permissions', config['user_permissions_sync_interval'])
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