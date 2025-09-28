# PowerBI Reports - SAR System v3

Esta carpeta contiene consultas SQL y herramientas para generar reportes para PowerBI desde el sistema SAR v3.

## 🚀 **Funcionalidad Principal**

El script `execute_query.sh` permite ejecutar consultas SQL en el contenedor de PostgreSQL del sistema SAR desde **fuera del contenedor**, utilizando las variables de entorno del contenedor para la conexión. Ideal para integración con **Ansible** y **PowerBI**.

## 📁 **Estructura de Archivos**

```
powerbi_reports/
├── execute_query.sh           # ⭐ Script principal para ejecutar consultas
├── powerbi_queries.sql        # 📊 Colección completa de consultas (10 categorías)
├── dashboard_metricas.sql     # 📈 Métricas para dashboard principal
├── usuarios_problemas_ad.sql  # ⚠️  Usuarios con problemas AD
├── solicitudes_pendientes.sql # ⏳ Solicitudes pendientes de aprobación
├── ansible_example.yml        # 🤖 Ejemplos de uso con Ansible
└── README.md                  # 📖 Esta documentación
```

## 🛠️ **Script de Ejecución**

### **Sintaxis**
```bash
./execute_query.sh <archivo.sql> [formato] [contenedor] [--quiet]
```

### **🔧 Parámetros**
- **`archivo.sql`**: Archivo con la consulta SQL a ejecutar (requerido)
- **`formato`**: Formato de salida - `csv`, `json`, `table`, `html` (por defecto: `csv`)
- **`contenedor`**: Nombre del contenedor DB (auto-detecta si no se especifica)
- **`--quiet`**: Modo silencioso - solo resultado en stdout, logs en stderr

### **📝 Ejemplos de Uso**

```bash
# ✅ Uso básico - CSV por defecto
./execute_query.sh dashboard_metricas.sql

# ✅ Formato específico
./execute_query.sh usuarios_problemas_ad.sql json

# ✅ Contenedor específico
./execute_query.sh solicitudes_pendientes.sql csv sar_v3-db-1

# ⭐ MODO ANSIBLE - Solo resultado limpio
./execute_query.sh dashboard_metricas.sql csv "" --quiet

# 🆘 Ayuda
./execute_query.sh --help
```

### **📊 Formatos de Salida**

| Formato | Descripción | Uso Principal |
|---------|-------------|---------------|
| **`csv`** | Valores separados por comas | 🎯 **PowerBI**, Excel, análisis |
| **`json`** | Formato JSON | APIs, aplicaciones web |
| **`table`** | Tabla formateada | 👀 Visualización en consola |
| **`html`** | Tabla HTML | Reportes web, emails |

### **🔥 Características Clave**

| Característica | Descripción |
|----------------|-------------|
| **🤖 Auto-detección** | Encuentra automáticamente el contenedor PostgreSQL |
| **🔐 Variables seguras** | Lee credenciales desde variables del contenedor |
| **✅ Validación completa** | Verifica archivos, parámetros y conectividad |
| **🔇 Modo silencioso** | Opción `--quiet` para integración con Ansible |
| **📝 Logging inteligente** | Logs detallados en stderr, datos en stdout |
| **⚡ Manejo de errores** | Códigos de salida apropiados y mensajes claros |

## 📊 **Consultas Disponibles**

### **🎯 Consultas Listas para Usar**

| Archivo | Descripción | Campos Clave | Uso PowerBI |
|---------|-------------|--------------|-------------|
| **`dashboard_metricas.sql`** | 📈 Métricas principales del sistema | usuarios_totales, carpetas_totales, solicitudes_pendientes | Dashboard principal |
| **`usuarios_problemas_ad.sql`** | ⚠️ Usuarios con problemas AD | username, ad_status, criticality_level, owned_folders | Alertas críticas |
| **`solicitudes_pendientes.sql`** | ⏳ Solicitudes pendientes | requester, folder_path, days_pending, possible_validators | Gestión de aprobaciones |
| **`usuarios_permisos_carpetas.sql`** | 🔐 Listado completo de permisos | usuario, carpeta (nombre), tipo_permiso, fecha_permiso, fuente_permiso | Auditoría de accesos |
| **`usuarios_estado_actual.sql`** | 👥 Estado actual de usuarios | usuario, nombre_completo, departamento, estado_ad, email | Inventario de usuarios |
| **`historico_solicitudes.sql`** | 📋 Histórico de solicitudes | solicitante_usuario, aprobador_usuario, carpeta, tipo_permiso, estado, fechas | Análisis de procesos |
| **`resumen_solicitudes.sql`** | 📊 Resumen estadístico de solicitudes | estado_solicitud, total_solicitudes, días_promedio_procesamiento | Dashboard de solicitudes |

### **📋 Colección Completa (`powerbi_queries.sql`)**

**10 categorías de consultas SQL organizadas:**

| Categoría | Descripción | Consultas Incluidas |
|-----------|-------------|-------------------|
| **1. 📊 Dashboard Principal** | Métricas generales y contadores | Usuarios, carpetas, grupos AD, solicitudes |
| **2. 👥 Usuarios y Estado AD** | Reportes de usuarios y problemas AD | Lista completa, usuarios críticos, estado AD |
| **3. 📁 Carpetas y Permisos** | Inventario de recursos y permisos | Lista de carpetas, detalle de permisos |
| **4. 📝 Solicitudes** | Histórico y gestión de solicitudes | Histórico completo, pendientes, métricas |
| **5. 🔐 Grupos AD** | Estado y membresías de grupos | Estado AD, impacto, membresías |
| **6. 📋 Auditoría** | Eventos y actividad del sistema | Eventos recientes, estadísticas |
| **7. 📈 Métricas y KPIs** | Indicadores de rendimiento | Tiempos de aprobación, top carpetas |
| **8. 🚨 Problemas y Alertas** | Detección de problemas | Carpetas sin propietarios, solicitudes antiguas |
| **9. 📊 Análisis de Tendencias** | Evolución temporal | Por mes, departamentos, patrones |
| **10. ✅ Verificación** | Checks de integridad del sistema | Problemas no reconocidos, inconsistencias |

## 🤖 **Integración con Ansible**

### **⭐ Modo Silencioso para Ansible**

El parámetro `--quiet` es **clave** para la integración con Ansible:
- **✅ stdout**: Solo datos CSV limpios
- **✅ stderr**: Solo errores críticos
- **❌ Sin logs**: No contamina la salida

### **📋 Ejemplo Básico**

```yaml
---
- name: Generar reporte PowerBI
  hosts: servidor_sar
  tasks:
    - name: Obtener métricas del sistema
      shell: |
        cd /ruta/al/proyecto/powerbi_reports
        ./execute_query.sh dashboard_metricas.sql csv "" --quiet
      register: metricas
      changed_when: false

    - name: Guardar CSV para PowerBI
      copy:
        content: "{{ metricas.stdout }}"
        dest: "/var/powerbi/metricas_{{ ansible_date_time.epoch }}.csv"
        mode: '0644'

    - name: Verificar datos obtenidos
      debug:
        msg: "✅ {{ metricas.stdout_lines | length - 1 }} filas generadas"
```

### **🔄 Ejemplo Avanzado - Múltiples Reportes**

```yaml
---
- name: Generar reportes PowerBI automatizados
  hosts: servidor_sar
  vars:
    reports_config:
      - name: "dashboard_metricas"
        sql_file: "dashboard_metricas.sql"
        description: "Métricas principales"
      - name: "usuarios_problemas_ad"
        sql_file: "usuarios_problemas_ad.sql"
        description: "Usuarios con problemas AD"
      - name: "solicitudes_pendientes"
        sql_file: "solicitudes_pendientes.sql"
        description: "Solicitudes pendientes"

  tasks:
    - name: Crear directorio de reportes
      file:
        path: "/var/powerbi/{{ ansible_date_time.date }}"
        state: directory
        mode: '0755'

    - name: Ejecutar consultas PowerBI
      shell: |
        cd /ruta/proyecto/powerbi_reports
        ./execute_query.sh {{ item.sql_file }} csv "" --quiet
      register: query_results
      loop: "{{ reports_config }}"
      changed_when: false

    - name: Guardar archivos CSV
      copy:
        content: "{{ item.stdout }}"
        dest: "/var/powerbi/{{ ansible_date_time.date }}/{{ reports_config[ansible_loop.index0].name }}.csv"
        mode: '0644'
      loop: "{{ query_results.results }}"
      loop_control:
        extended: true

    - name: Resumen de archivos generados
      debug:
        msg:
          - "📊 Reportes generados en: /var/powerbi/{{ ansible_date_time.date }}/"
          - "{% for config in reports_config %}✅ {{ config.name }}.csv - {{ config.description }}{% endfor %}"
```

### **⏰ Automatización con Cron**

```bash
# Reportes diarios para PowerBI (6:00 AM)
0 6 * * * cd /ruta/proyecto/powerbi_reports && ./execute_query.sh dashboard_metricas.sql csv "" --quiet > /var/powerbi/daily/metricas_$(date +\%Y\%m\%d).csv 2>/var/log/powerbi.log

# Reportes de problemas AD (7:00 AM)
0 7 * * * cd /ruta/proyecto/powerbi_reports && ./execute_query.sh usuarios_problemas_ad.sql csv "" --quiet > /var/powerbi/daily/problemas_ad_$(date +\%Y\%m\%d).csv 2>>/var/log/powerbi.log

# Reportes semanales (Lunes 8:00 AM)
0 8 * * 1 cd /ruta/proyecto/powerbi_reports && ./execute_query.sh solicitudes_pendientes.sql csv "" --quiet > /var/powerbi/weekly/solicitudes_$(date +\%Y\%m\%d).csv 2>>/var/log/powerbi.log
```

## Requisitos

1. **Docker**: El script requiere acceso a Docker para ejecutar comandos en el contenedor
2. **Contenedor DB**: El contenedor de PostgreSQL debe estar en ejecución
3. **Permisos**: El usuario debe tener permisos para ejecutar `docker exec`
4. **Variables de entorno**: El contenedor debe tener configuradas las variables `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

## Solución de Problemas

### Error: "No se encontraron contenedores de base de datos"
- Verificar que el contenedor de PostgreSQL está ejecutándose: `docker ps`
- Especificar manualmente el nombre del contenedor como tercer parámetro

### Error: "No se pudieron obtener las variables de entorno"
- Verificar que el contenedor tiene las variables configuradas: `docker exec <contenedor> printenv | grep POSTGRES`
- Revisar el archivo `.env` y `docker-compose.yml`

### Error en la ejecución de consultas
- Verificar la sintaxis SQL del archivo de consulta
- Comprobar que las tablas existen en la base de datos
- Revisar los logs del contenedor: `docker logs <contenedor>`

## Personalización

Para crear nuevas consultas:

1. Crear un archivo `.sql` con la consulta
2. Añadir comentarios descriptivos al inicio
3. Probar la consulta con el script
4. Documentar en este README si es necesario

### Ejemplo de Archivo de Consulta

```sql
-- =============================================================================
-- CONSULTA: NOMBRE DESCRIPTIVO
-- =============================================================================
-- Descripción: Breve descripción de qué hace la consulta
-- Uso: ./execute_query.sh mi_consulta.sql
-- =============================================================================

SELECT
    columna1,
    columna2
FROM tabla
WHERE condicion = 'valor';
```

## Integración con PowerBI

1. **Importación de datos**: Usar la salida CSV directamente en PowerBI
2. **Automatización**: Configurar scripts para generar archivos periódicamente
3. **Fuente de datos**: Configurar PowerBI para leer archivos CSV desde una ubicación específica
4. **Actualización**: Programar la actualización automática de datos en PowerBI

## Seguridad

- Las credenciales de base de datos se obtienen del contenedor, no se almacenan en el script
- Los archivos temporales se crean en `/tmp` dentro del contenedor
- Se recomienda ejecutar en un entorno seguro con acceso controlado a Docker