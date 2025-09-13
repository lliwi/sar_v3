#!/usr/bin/env python3
"""
Script para procesar tareas pendientes de forma periódica
"""
import time
import os
import sys
from datetime import datetime
import zoneinfo

# Añadir el directorio de la aplicación al path
sys.path.insert(0, '/app')

# Configurar timezone según variable de entorno
TZ = os.getenv('TZ', 'Europe/Madrid')
try:
    LOCAL_TIMEZONE = zoneinfo.ZoneInfo(TZ)
except Exception:
    LOCAL_TIMEZONE = None

from app import create_app
from app.services.task_service import TaskService

def get_local_time():
    """Obtener tiempo local según configuración TZ"""
    if LOCAL_TIMEZONE:
        return datetime.now(LOCAL_TIMEZONE)
    else:
        return datetime.now()

def main():
    """Función principal del planificador de tareas"""
    app = create_app()

    # Obtener intervalo de procesamiento de variables de entorno
    interval = int(os.getenv('TASK_PROCESSING_INTERVAL', 300))  # 5 minutos por defecto

    print(f'🚀 Iniciando planificador de tareas SAR v3', flush=True)
    print(f'⏱️  Intervalo de procesamiento: {interval} segundos', flush=True)
    print(f'⏰ Timezone configurado: {TZ}', flush=True)
    print(f'📅 Fecha de inicio: {get_local_time().strftime("%Y-%m-%d %H:%M:%S %Z")}', flush=True)
    print('-' * 60, flush=True)
    
    while True:
        try:
            local_time = get_local_time()
            current_time = local_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f'[{current_time}] 🔄 Iniciando ciclo de procesamiento...', flush=True)

            with app.app_context():
                # Debug: verificar tareas disponibles
                from app.models import Task
                from datetime import datetime as dt

                current_utc = dt.utcnow()
                print(f'[{current_time}] 🕐 Tiempo UTC: {current_utc}', flush=True)

                ready_tasks = Task.query.filter(
                    Task.status.in_(['pending', 'retry']),
                    Task.next_execution_at <= current_utc
                ).all()

                print(f'[{current_time}] 📋 Tareas listas para ejecutar: {len(ready_tasks)}', flush=True)
                for task in ready_tasks:
                    print(f'[{current_time}] 📌 Tarea {task.id}: {task.name} - {task.status} - Programada: {task.next_execution_at}', flush=True)

                # Procesar tareas
                task_service = TaskService()
                processed_count = task_service.process_pending_tasks()

                print(f'[{current_time}] ✅ Procesadas {processed_count} tareas', flush=True)

        except Exception as e:
            current_time = get_local_time().strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f'[{current_time}] ❌ Error procesando tareas: {str(e)}', flush=True)
            import traceback
            traceback.print_exc()

        # Esperar antes del siguiente procesamiento
        print(f'[{current_time}] ⏰ Esperando {interval} segundos hasta el próximo ciclo...', flush=True)

        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print(f'[{current_time}] 🛑 Interrupción recibida, deteniendo scheduler...', flush=True)
            break
        except Exception as sleep_error:
            print(f'[{current_time}] ⚠️ Error en sleep: {sleep_error}', flush=True)
            time.sleep(5)  # Fallback corto

if __name__ == '__main__':
    main()