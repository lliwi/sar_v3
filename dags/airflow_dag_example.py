"""
DAG de ejemplo para SAR v3 - Procesamiento de permisos
Este DAG recibe parámetros via la API de Airflow y procesa archivos CSV de cambios de permisos.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import csv
import logging
import os

# Configuración por defecto del DAG
default_args = {
    'owner': 'sar-system',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

# Definición del DAG
dag = DAG(
    'SAR_V3',
    default_args=default_args,
    description='SAR v3 - Procesamiento de cambios de permisos en Active Directory',
    schedule_interval=None,  # Solo se ejecuta manualmente via API
    catchup=False,
    tags=['sar', 'permissions', 'active-directory']
)

def print_dag_parameters(**context):
    """
    Función que extrae y imprime los parámetros pasados al DAG via API
    Los parámetros se reciben en context['dag_run'].conf
    """
    logging.info("=== INICIO: Procesamiento de parámetros del DAG ===")
    
    # Obtener la configuración pasada via API
    dag_run = context.get('dag_run')
    if not dag_run:
        logging.error("No se encontró dag_run en el contexto")
        return False
    
    conf = dag_run.conf or {}
    logging.info(f"Configuración recibida: {conf}")
    
    # Extraer parámetros específicos
    change_file = conf.get('change_file')
    request_ids = conf.get('request_ids', [])
    triggered_by = conf.get('triggered_by', 'unknown')
    
    # Imprimir parámetros recibidos
    logging.info(f"📁 Archivo de cambios: {change_file}")
    logging.info(f"🆔 IDs de solicitudes: {request_ids}")
    logging.info(f"👤 Ejecutado por: {triggered_by}")
    
    # Verificar si el archivo existe
    if change_file:
        if os.path.exists(change_file):
            logging.info(f"✅ Archivo encontrado: {change_file}")
            
            # Leer y mostrar contenido del CSV (primeras 5 líneas como ejemplo)
            try:
                with open(change_file, 'r', encoding='utf-8') as file:
                    reader = csv.reader(file)
                    lines = list(reader)
                    
                logging.info(f"📊 Total de líneas en CSV: {len(lines)}")
                logging.info("📋 Primeras líneas del archivo:")
                
                for i, line in enumerate(lines[:5]):  # Mostrar solo primeras 5 líneas
                    logging.info(f"   Línea {i+1}: {line}")
                    
            except Exception as e:
                logging.error(f"❌ Error leyendo archivo CSV: {str(e)}")
        else:
            logging.error(f"❌ Archivo no encontrado: {change_file}")
    else:
        logging.warning("⚠️  No se especificó archivo de cambios")
    
    # Almacenar parámetros en XCom para otras tareas
    context['task_instance'].xcom_push(key='change_file', value=change_file)
    context['task_instance'].xcom_push(key='request_ids', value=request_ids)
    context['task_instance'].xcom_push(key='triggered_by', value=triggered_by)
    
    logging.info("=== FIN: Procesamiento de parámetros completado ===")
    return True

def process_permission_changes(**context):
    """
    Función que simula el procesamiento de cambios de permisos
    En una implementación real, aquí se aplicarían los cambios en Active Directory
    """
    logging.info("=== INICIO: Procesamiento de cambios de permisos ===")
    
    # Obtener parámetros de la tarea anterior via XCom
    ti = context['task_instance']
    change_file = ti.xcom_pull(key='change_file', task_ids='print_parameters')
    request_ids = ti.xcom_pull(key='request_ids', task_ids='print_parameters')
    triggered_by = ti.xcom_pull(key='triggered_by', task_ids='print_parameters')
    
    logging.info(f"Procesando archivo: {change_file}")
    logging.info(f"Solicitudes: {request_ids}")
    logging.info(f"Usuario: {triggered_by}")
    
    if not change_file or not os.path.exists(change_file):
        logging.error("❌ No se puede procesar: archivo no disponible")
        return False
    
    try:
        # Leer y procesar el archivo CSV
        with open(change_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            changes_processed = 0
            
            for row in reader:
                # Simular procesamiento de cada cambio
                action = row.get('action')
                folder_path = row.get('folder_path')
                ad_group = row.get('ad_group_name')
                permission_type = row.get('permission_type')
                
                logging.info(f"🔧 Procesando: {action} - {permission_type} para {ad_group} en {folder_path}")
                
                # Aquí iría la lógica real de aplicar cambios en AD
                # Por ahora solo simulamos con logs
                changes_processed += 1
            
            logging.info(f"✅ Procesados {changes_processed} cambios de permisos")
            
    except Exception as e:
        logging.error(f"❌ Error procesando cambios: {str(e)}")
        return False
    
    logging.info("=== FIN: Procesamiento de cambios completado ===")
    return True

def cleanup_files(**context):
    """
    Función para limpiar archivos temporales después del procesamiento
    """
    logging.info("=== INICIO: Limpieza de archivos temporales ===")
    
    ti = context['task_instance']
    change_file = ti.xcom_pull(key='change_file', task_ids='print_parameters')
    
    if change_file and os.path.exists(change_file):
        try:
            # En producción, podrías querer mover el archivo a un directorio de backup
            # en lugar de eliminarlo directamente
            backup_dir = "/app/exports/processed"
            os.makedirs(backup_dir, exist_ok=True)
            
            import shutil
            backup_file = os.path.join(backup_dir, os.path.basename(change_file))
            shutil.move(change_file, backup_file)
            
            logging.info(f"📦 Archivo movido a backup: {backup_file}")
            
        except Exception as e:
            logging.error(f"❌ Error en limpieza: {str(e)}")
    
    logging.info("=== FIN: Limpieza completada ===")
    return True

def run_remote_ansible_command(**context):
    """
    Función que ejecuta un playbook de Ansible via SSH para aplicar cambios de permisos
    Obtiene el path de la carpeta de los parámetros del DAG
    """
    logging.info("=== INICIO: Ejecución de comando Ansible remoto ===")
    
    # Obtener parámetros de la tarea anterior via XCom
    ti = context['task_instance']
    change_file = ti.xcom_pull(key='change_file', task_ids='print_parameters')
    triggered_by = ti.xcom_pull(key='triggered_by', task_ids='print_parameters')
    
    # Obtener configuración pasada via API para extraer folder_path
    dag_run = context.get('dag_run')
    conf = dag_run.conf or {} if dag_run else {}
    folder_path = conf.get('folder_path', '')
    
    logging.info(f"📁 Ejecutando Ansible para carpeta: {folder_path}")
    logging.info(f"👤 Solicitado por: {triggered_by}")
    logging.info(f"📄 Basado en archivo: {change_file}")
    
    if not folder_path:
        logging.error("❌ No se especificó folder_path en la configuración del DAG")
        return False
    
    try:
        # Definir el SSHHook
        ssh_hook = SSHHook(ssh_conn_id='ssh_IGESBCNLSV00002', cmd_timeout=None)
        
        # Preparar el comando Ansible con el path de la carpeta como variable extra
        ansible_command = (
            'ANSIBLE_CONFIG=/opt/ansible/ansible.cfg '
            '/usr/people/idg/tmomill-adm/.local/bin/ansible-playbook '
            '-i /opt/ansible/hosts/inventory.yaml '
            f'-e "target_folder_path={folder_path}" '
            '/opt/ansible/playbooks/play-SARV3.yaml'
        )
        
        logging.info(f"🔧 Comando Ansible a ejecutar: {ansible_command}")
        
        # Ejecutar el comando SSH
        ssh_client = ssh_hook.get_conn()
        stdin, stdout, stderr = ssh_client.exec_command(ansible_command)
        exit_status = stdout.channel.recv_exit_status()
        
        # Capturar la salida y los errores
        output = stdout.read().decode('utf-8')
        error_output = stderr.read().decode('utf-8')
        
        logging.info(f"📊 Estado de salida: {exit_status}")
        logging.info(f"📋 Salida del comando:")
        logging.info(output)
        
        if error_output:
            logging.warning(f"⚠️ Errores capturados:")
            logging.warning(error_output)
        
        # Detectar errores de ansible analizando la salida
        if exit_status != 0:
            logging.error(f"❌ El comando Ansible falló con código de salida: {exit_status}")
            return False
            
        lines = output.split('\n')
        for line in lines:
            if "failed=" in line:
                # Extraer el nombre al inicio de la línea
                name = line.split()[0] if line.split() else "unknown"
                # Extraer el valor de failed
                try:
                    failed_part = line.split("failed=")[1]
                    failed_count = int(failed_part.split()[0])
                    if failed_count > 0:
                        error_msg = f"❌ Ansible falló en host: {name} con {failed_count} errores"
                        logging.error(error_msg)
                        raise ValueError(error_msg)
                except (IndexError, ValueError) as e:
                    logging.warning(f"⚠️ Error parseando línea de fallos: {line} - {str(e)}")
        
        logging.info("✅ Comando Ansible ejecutado exitosamente")
        
        # Almacenar resultado en XCom para otras tareas
        context['task_instance'].xcom_push(key='ansible_output', value=output)
        context['task_instance'].xcom_push(key='ansible_exit_status', value=exit_status)
        
    except Exception as e:
        logging.error(f"❌ Error ejecutando comando Ansible: {str(e)}")
        return False
    finally:
        try:
            ssh_client.close()
        except:
            pass
    
    logging.info("=== FIN: Ejecución de comando Ansible completada ===")
    return True

# Definición de tareas
task_print_parameters = PythonOperator(
    task_id='print_parameters',
    python_callable=print_dag_parameters,
    dag=dag,
    doc_md="""
    ## Extraer Parámetros
    
    Esta tarea extrae y muestra los parámetros pasados al DAG via API:
    - `change_file`: Ruta al archivo CSV con los cambios
    - `request_ids`: Lista de IDs de solicitudes de permisos
    - `triggered_by`: Usuario que inició la ejecución
    """
)

task_process_changes = PythonOperator(
    task_id='process_permission_changes',
    python_callable=process_permission_changes,
    dag=dag,
    doc_md="""
    ## Procesar Cambios
    
    Esta tarea procesa el archivo CSV y aplica los cambios de permisos.
    En una implementación real, aquí se conectaría con Active Directory.
    """
)

task_cleanup = PythonOperator(
    task_id='cleanup_files',
    python_callable=cleanup_files,
    dag=dag,
    doc_md="""
    ## Limpieza
    
    Esta tarea mueve el archivo procesado a un directorio de backup
    para mantener limpio el directorio de exports.
    """
)

# Tarea SSH para ejecutar Ansible
task_run_ansible = PythonOperator(
    task_id='run_remote_ansible',
    python_callable=run_remote_ansible_command,
    dag=dag,
    doc_md="""
    ## Ejecutar Ansible Remoto
    
    Esta tarea ejecuta un playbook de Ansible via SSH para aplicar cambios de permisos en el servidor remoto.
    
    **Parámetros utilizados:**
    - `folder_path`: Ruta de la carpeta donde aplicar cambios (pasada como variable extra a Ansible)
    - `change_file`: Archivo CSV con los cambios procesados
    - `triggered_by`: Usuario que inició la operación
    
    **Playbook ejecutado:** `/opt/ansible/playbooks/play-SARV3.yaml`
    """
)

# Tarea adicional de verificación usando BashOperator
task_health_check = BashOperator(
    task_id='health_check',
    bash_command='echo "🏥 DAG execution completed successfully at $(date)"',
    dag=dag
)

# Definir dependencias
task_print_parameters >> task_process_changes >> task_run_ansible >> task_cleanup >> task_health_check

# Documentación del DAG
dag.doc_md = """
# SAR v3 - DAG de Procesamiento de Permisos

Este DAG procesa las solicitudes de cambios de permisos aprobadas en el sistema SAR.

## Parámetros de Entrada (via API)

El DAG espera recibir los siguientes parámetros en el campo `conf` al ser ejecutado via API:

```json
{
    "change_file": "/app/exports/permission_changes_20240101_120000.csv",
    "request_ids": [123, 124, 125],
    "triggered_by": "admin.usuario",
    "folder_path": "\\\\servidor\\carpeta\\subcarpeta"
}
```

## Flujo de Ejecución

1. **print_parameters**: Extrae y muestra los parámetros recibidos
2. **process_permission_changes**: Procesa los cambios del archivo CSV
3. **run_remote_ansible**: Ejecuta playbook de Ansible para aplicar cambios en servidor remoto
4. **cleanup_files**: Mueve el archivo procesado a backup
5. **health_check**: Verificación final de salud

## Ejemplo de Uso desde SAR

```python
# Desde el AirflowService
conf = {
    'change_file': '/app/exports/permission_changes_20240101_120000.csv',
    'request_ids': [123, 124, 125],
    'triggered_by': 'sistema.sar',
    'folder_path': '\\\\servidor\\carpeta\\target'
}
success = airflow_service.trigger_dag(conf)
```
"""