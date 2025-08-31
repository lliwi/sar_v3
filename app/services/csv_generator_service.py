"""
Servicio para generar archivos CSV para cambios de permisos en Active Directory.
Este servicio genera los archivos CSV que son procesados por el script PowerShell
y ejecutados a través del DAG de Airflow.
"""

import csv
import os
import uuid
from datetime import datetime
from typing import List, Dict, Optional
from flask import current_app
from app.models import PermissionRequest, User, ADGroup, FolderPermission


class CSVGeneratorService:
    """Servicio para generar archivos CSV de cambios de permisos"""
    
    def __init__(self):
        self.csv_delimiter = ";"
        self.csv_output_dir = current_app.config.get('CSV_OUTPUT_DIR', '/tmp/sar_csv_files')
        self.output_directory = self.csv_output_dir  # Mantener compatibilidad
        
        # Crear directorio si no existe
        os.makedirs(self.csv_output_dir, exist_ok=True)
    
    def generate_permission_change_csv(self, permission_request: PermissionRequest, action: str) -> str:
        """
        Genera un archivo CSV para un cambio de permiso específico.
        
        Args:
            permission_request: La solicitud de permiso aprobada
            action: 'add' o 'remove'
        
        Returns:
            str: Ruta del archivo CSV generado
        """
        if not permission_request.ad_group:
            raise ValueError("No se puede generar CSV sin grupo AD asignado")
            
        # Generar nombre único para el archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"membershipChange_{timestamp}_{unique_id}.csv"
        file_path = os.path.join(self.output_directory, filename)
        
        # Determinar acción (1=agregar, 2=eliminar)
        action_id = 1 if action == 'add' else 2
        
        # Preparar datos para el CSV
        csv_data = self._prepare_csv_row(permission_request, action_id)
        
        # Escribir archivo CSV
        self._write_csv_file(file_path, [csv_data])
        
        current_app.logger.info(f"CSV generado: {file_path} para solicitud {permission_request.id}")
        
        return file_path
    
    def generate_bulk_changes_csv(self, changes: List[Dict]) -> str:
        """
        Genera un archivo CSV para múltiples cambios de permisos.
        
        Args:
            changes: Lista de diccionarios con cambios
                    Formato: {'permission_request': PermissionRequest, 'action': 'add'|'remove'}
        
        Returns:
            str: Ruta del archivo CSV generado
        """
        if not changes:
            raise ValueError("No se proporcionaron cambios para generar el CSV")
            
        # Generar nombre único para el archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"bulkMembershipChanges_{timestamp}_{unique_id}.csv"
        file_path = os.path.join(self.output_directory, filename)
        
        # Preparar todos los datos
        csv_data = []
        for change in changes:
            permission_request = change['permission_request']
            action = change['action']
            action_id = 1 if action == 'add' else 2
            
            if permission_request.ad_group:
                csv_data.append(self._prepare_csv_row(permission_request, action_id))
        
        if not csv_data:
            raise ValueError("No se pudieron preparar datos para el CSV")
            
        # Escribir archivo CSV
        self._write_csv_file(file_path, csv_data)
        
        current_app.logger.info(f"CSV bulk generado: {file_path} con {len(csv_data)} cambios")
        
        return file_path
    
    def _prepare_csv_row(self, permission_request: PermissionRequest, action_id: int) -> Dict:
        """
        Prepara una fila de datos para el CSV.
        
        Args:
            permission_request: Solicitud de permiso
            action_id: 1 para agregar, 2 para eliminar
        
        Returns:
            Dict: Datos de la fila
        """
        user = permission_request.requester
        ad_group = permission_request.ad_group
        
        # Formatear nombre de usuario (sin dominio)
        username = user.username
        if '\\' in username:
            username = username.split('\\')[1]
        
        # Formatear grupo AD (con prefijo de dominio configurable)
        ad_domain_prefix = os.getenv('AD_DOMAIN_PREFIX', '')
        group_name = ad_group.name
        if ad_domain_prefix and not group_name.startswith(f'{ad_domain_prefix}\\'):
            group_name = f"{ad_domain_prefix}\\{group_name}"
        
        # Obtener matrícula del usuario (asumiendo que está en el campo employee_id)
        matricula = getattr(user, 'employee_id', user.id)
        
        # Obtener IDs de recurso y modo
        folder_id = permission_request.folder_id
        mode_id = 1 if permission_request.permission_type == 'read' else 2  # 1=lectura, 2=escritura
        
        return {
            'UserName': username,
            'ADGroup': group_name,
            'idTarea': permission_request.id,
            'idAccion': action_id,
            'MatriculaUsu': matricula,
            'idRecurso': folder_id,
            'idModo': mode_id
        }
    
    def _write_csv_file(self, file_path: str, data: List[Dict]) -> None:
        """
        Escribe los datos al archivo CSV con el formato requerido.
        
        Args:
            file_path: Ruta del archivo
            data: Lista de diccionarios con los datos
        """
        fieldnames = ['UserName', 'ADGroup', 'idTarea', 'idAccion', 'MatriculaUsu', 'idRecurso', 'idModo']
        
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=self.csv_delimiter)
            
            # Escribir encabezados
            writer.writeheader()
            
            # Escribir datos
            for row in data:
                writer.writerow(row)
    
    def get_csv_file_info(self, file_path: str) -> Dict:
        """
        Obtiene información sobre un archivo CSV generado.
        
        Args:
            file_path: Ruta del archivo CSV
        
        Returns:
            Dict: Información del archivo
        """
        if not os.path.exists(file_path):
            return None
            
        stat = os.stat(file_path)
        
        return {
            'file_path': file_path,
            'filename': os.path.basename(file_path),
            'size': stat.st_size,
            'created_at': datetime.fromtimestamp(stat.st_ctime),
            'modified_at': datetime.fromtimestamp(stat.st_mtime)
        }
    
    def cleanup_old_csv_files(self, days_old: int = 7) -> int:
        """
        Limpia archivos CSV antiguos.
        
        Args:
            days_old: Días de antigüedad para eliminar archivos
        
        Returns:
            int: Número de archivos eliminados
        """
        import time
        
        now = time.time()
        cutoff_time = now - (days_old * 24 * 60 * 60)
        
        deleted_count = 0
        
        try:
            for filename in os.listdir(self.output_directory):
                if filename.endswith('.csv'):
                    file_path = os.path.join(self.output_directory, filename)
                    if os.path.getctime(file_path) < cutoff_time:
                        os.remove(file_path)
                        deleted_count += 1
                        current_app.logger.info(f"Archivo CSV eliminado: {filename}")
        except Exception as e:
            current_app.logger.error(f"Error limpiando archivos CSV: {e}")
        
        return deleted_count
    
    def generate_removal_csv_from_folder_permissions(self, folder_id: int, user_id: int, permission_type: str) -> str:
        """
        Genera CSV para eliminar permisos existentes de una carpeta.
        Útil cuando se necesita revocar un permiso directo sin solicitud.
        
        Args:
            folder_id: ID de la carpeta
            user_id: ID del usuario
            permission_type: Tipo de permiso ('read' o 'write')
        
        Returns:
            str: Ruta del archivo CSV generado
        """
        from app.models import Folder
        
        folder = Folder.query.get(folder_id)
        user = User.query.get(user_id)
        
        if not folder or not user:
            raise ValueError("Carpeta o usuario no encontrados")
        
        # Buscar los grupos AD asociados a esta carpeta y tipo de permiso
        folder_permissions = FolderPermission.query.filter_by(
            folder_id=folder_id,
            permission_type=permission_type,
            is_active=True
        ).all()
        
        if not folder_permissions:
            raise ValueError(f"No se encontraron permisos de {permission_type} para la carpeta {folder.name}")
        
        # Generar archivo CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"removePermission_{timestamp}_{unique_id}.csv"
        file_path = os.path.join(self.output_directory, filename)
        
        csv_data = []
        
        for fp in folder_permissions:
            # Crear entrada temporal para generar el CSV
            username = user.username
            if '\\' in username:
                username = username.split('\\')[1]
            
            ad_domain_prefix = os.getenv('AD_DOMAIN_PREFIX', '')
            group_name = fp.ad_group.name
            if ad_domain_prefix and not group_name.startswith(f'{ad_domain_prefix}\\'):
                group_name = f"{ad_domain_prefix}\\{group_name}"
            
            matricula = getattr(user, 'employee_id', user.id)
            mode_id = 1 if permission_type == 'read' else 2
            
            csv_data.append({
                'UserName': username,
                'ADGroup': group_name,
                'idTarea': f"REMOVE_{folder_id}_{user_id}_{unique_id}",
                'idAccion': 2,  # 2 = eliminar
                'MatriculaUsu': matricula,
                'idRecurso': folder_id,
                'idModo': mode_id
            })
        
        # Escribir archivo CSV
        self._write_csv_file(file_path, csv_data)
        
        current_app.logger.info(f"CSV de eliminación generado: {file_path}")
        
        return file_path
    
    def generate_ad_sync_removal_csv(self, user: User, folder, ad_group: ADGroup, permission_type: str) -> str:
        """
        Genera CSV para eliminar permisos sincronizados desde AD.
        
        Args:
            user: Usuario del que se va a eliminar el permiso
            folder: Carpeta de la que se elimina el permiso
            ad_group: Grupo AD del que se elimina el permiso
            permission_type: Tipo de permiso ('read' o 'write')
        
        Returns:
            str: Ruta del archivo CSV generado
        """
        # Generar archivo CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        filename = f"removeADSyncPermission_{timestamp}_{unique_id}.csv"
        file_path = os.path.join(self.output_directory, filename)
        
        # Formatear nombre de usuario
        username = user.username
        if '\\' in username:
            username = username.split('\\')[1]
        
        # Formatear grupo AD
        ad_domain_prefix = os.getenv('AD_DOMAIN_PREFIX', '')
        group_name = ad_group.name
        if ad_domain_prefix and not group_name.startswith(f'{ad_domain_prefix}\\'):
            group_name = f"{ad_domain_prefix}\\{group_name}"
        
        matricula = getattr(user, 'employee_id', user.id)
        mode_id = 1 if permission_type == 'read' else 2
        
        csv_data = [{
            'UserName': username,
            'ADGroup': group_name,
            'idTarea': f"REMOVE_AD_SYNC_{folder.id}_{user.id}_{unique_id}",
            'idAccion': 2,  # 2 = eliminar
            'MatriculaUsu': matricula,
            'idRecurso': folder.id,
            'idModo': mode_id
        }]
        
        # Escribir archivo CSV
        self._write_csv_file(file_path, csv_data)
        
        current_app.logger.info(f"CSV de eliminación AD sync generado: {file_path}")
        
        return file_path