#!/usr/bin/env python3
"""
Script para procesar tareas pendientes de forma periódica
"""
import time
import os
import sys
from datetime import datetime

# Añadir el directorio de la aplicación al path
sys.path.insert(0, '/app')

from app import create_app
from app.services.task_service import TaskService

def main():
    """Función principal del planificador de tareas"""
    app = create_app()
    
    # Obtener intervalo de procesamiento de variables de entorno
    interval = int(os.getenv('TASK_PROCESSING_INTERVAL', 300))  # 5 minutos por defecto
    
    print(f'🚀 Iniciando planificador de tareas SAR v3')
    print(f'⏱️  Intervalo de procesamiento: {interval} segundos')
    print(f'📅 Fecha de inicio: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print('-' * 60)
    
    while True:
        try:
            with app.app_context():
                task_service = TaskService()
                processed_count = task_service.process_pending_tasks()
                
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f'[{current_time}] ✅ Procesadas {processed_count} tareas')
                
        except Exception as e:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f'[{current_time}] ❌ Error procesando tareas: {str(e)}')
            import traceback
            traceback.print_exc()
        
        # Esperar antes del siguiente procesamiento
        time.sleep(interval)

if __name__ == '__main__':
    main()