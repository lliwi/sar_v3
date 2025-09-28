#!/bin/bash

# =============================================================================
# SCRIPT PARA EJECUTAR CONSULTAS SQL EN EL CONTENEDOR DE BASE DE DATOS
# =============================================================================
# Este script ejecuta consultas SQL en el contenedor PostgreSQL del sistema SAR
# Utiliza las variables de entorno del contenedor para la conexión
#
# Uso: ./execute_query.sh <archivo_consulta.sql> [formato_salida] [contenedor_db] [--quiet]
#
# Parámetros:
#   archivo_consulta.sql: Archivo con la consulta SQL a ejecutar
#   formato_salida: csv, json, table (por defecto: csv)
#   contenedor_db: Nombre del contenedor DB (por defecto: auto-detectar)
#   --quiet: Solo mostrar resultado, sin logs (ideal para Ansible)
#
# Ejemplos:
#   ./execute_query.sh usuarios_con_problemas_ad.sql
#   ./execute_query.sh dashboard_metricas.sql json
#   ./execute_query.sh reporte_carpetas.sql csv sar_v3-db-1
#   ./execute_query.sh dashboard_metricas.sql csv "" --quiet
# =============================================================================

set -euo pipefail

# Colores para la salida
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Detectar modo silencioso temprano
QUIET_MODE=false
for arg in "$@"; do
    if [[ "$arg" == "--quiet" ]]; then
        QUIET_MODE=true
        break
    fi
done

# Función para mostrar ayuda
show_help() {
    echo -e "${BLUE}==============================================================================${NC}"
    echo -e "${BLUE}SCRIPT PARA EJECUTAR CONSULTAS SQL EN EL CONTENEDOR DE BASE DE DATOS${NC}"
    echo -e "${BLUE}==============================================================================${NC}"
    echo ""
    echo -e "${GREEN}Uso:${NC} $0 <archivo_consulta.sql> [formato_salida] [contenedor_db] [--quiet]"
    echo ""
    echo -e "${GREEN}Parámetros:${NC}"
    echo "  archivo_consulta.sql : Archivo con la consulta SQL a ejecutar"
    echo "  formato_salida       : csv, json, table, html (por defecto: csv)"
    echo "  contenedor_db        : Nombre del contenedor DB (por defecto: auto-detectar)"
    echo "  --quiet              : Solo mostrar resultado, sin logs (ideal para Ansible)"
    echo ""
    echo -e "${GREEN}Ejemplos:${NC}"
    echo "  $0 usuarios_con_problemas_ad.sql"
    echo "  $0 dashboard_metricas.sql json"
    echo "  $0 reporte_carpetas.sql csv sar_v3-db-1"
    echo "  $0 dashboard_metricas.sql csv \"\" --quiet"
    echo ""
    echo -e "${GREEN}Formatos de salida disponibles:${NC}"
    echo "  csv   : Valores separados por comas (por defecto)"
    echo "  json  : Formato JSON"
    echo "  table : Tabla formateada para consola"
    echo "  html  : Tabla HTML"
    echo ""
    exit 0
}

# Función para logging
log() {
    if [[ "$QUIET_MODE" != "true" ]]; then
        echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" >&2
    fi
}

log_error() {
    echo -e "${RED}[ERROR $(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" >&2
}

log_success() {
    if [[ "$QUIET_MODE" != "true" ]]; then
        echo -e "${GREEN}[SUCCESS $(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" >&2
    fi
}

log_warning() {
    if [[ "$QUIET_MODE" != "true" ]]; then
        echo -e "${YELLOW}[WARNING $(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" >&2
    fi
}

# Verificar si se solicita ayuda
if [[ ${1:-} == "-h" ]] || [[ ${1:-} == "--help" ]] || [[ $# -eq 0 ]]; then
    show_help
fi

# Ya detectado arriba

# Validar parámetros
if [[ $# -lt 1 ]]; then
    log_error "Se requiere al menos un parámetro: archivo de consulta SQL"
    show_help
fi

# Parámetros (filtrar --quiet)
ARGS=()
for arg in "$@"; do
    if [[ "$arg" != "--quiet" ]]; then
        ARGS+=("$arg")
    fi
done

QUERY_FILE="${ARGS[0]}"
OUTPUT_FORMAT="${ARGS[1]:-csv}"
DB_CONTAINER="${ARGS[2]:-}"

# Validar archivo de consulta
if [[ ! -f "$QUERY_FILE" ]]; then
    log_error "El archivo de consulta '$QUERY_FILE' no existe"
    exit 1
fi

# Validar formato de salida
case "$OUTPUT_FORMAT" in
    csv|json|table|html)
        ;;
    *)
        log_error "Formato de salida no válido: '$OUTPUT_FORMAT'. Use: csv, json, table, html"
        exit 1
        ;;
esac

log "Iniciando ejecución de consulta SQL..."
log "Archivo: $QUERY_FILE"
log "Formato: $OUTPUT_FORMAT"

# Auto-detectar contenedor de base de datos si no se especifica
if [[ -z "$DB_CONTAINER" ]]; then
    log "Auto-detectando contenedor de base de datos..."

    # Buscar contenedores que contengan "db" y estén ejecutándose
    DB_CANDIDATES=$(docker ps --format "table {{.Names}}" | grep -E "(db|postgres)" | head -5)

    if [[ -z "$DB_CANDIDATES" ]]; then
        log_error "No se encontraron contenedores de base de datos en ejecución"
        log_error "Contenedores disponibles:"
        docker ps --format "table {{.Names}}\t{{.Status}}" >&2
        exit 1
    fi

    # Tomar el primer candidato
    DB_CONTAINER=$(echo "$DB_CANDIDATES" | head -1)
    log "Contenedor detectado: $DB_CONTAINER"
else
    log "Usando contenedor especificado: $DB_CONTAINER"
fi

# Verificar que el contenedor existe y está ejecutándose
if ! docker ps --format "{{.Names}}" | grep -q "^${DB_CONTAINER}$"; then
    log_error "El contenedor '$DB_CONTAINER' no existe o no está ejecutándose"
    log_error "Contenedores disponibles:"
    docker ps --format "table {{.Names}}\t{{.Status}}" >&2
    exit 1
fi

# Función para obtener variables de entorno del contenedor
get_env_var() {
    local var_name="$1"
    docker exec "$DB_CONTAINER" printenv "$var_name" 2>/dev/null || echo ""
}

# Obtener variables de entorno de la base de datos
log "Obteniendo configuración de base de datos del contenedor..."

POSTGRES_DB=$(get_env_var "POSTGRES_DB")
POSTGRES_USER=$(get_env_var "POSTGRES_USER")
POSTGRES_PASSWORD=$(get_env_var "POSTGRES_PASSWORD")

# Validar que se obtuvieron las variables necesarias
if [[ -z "$POSTGRES_DB" ]] || [[ -z "$POSTGRES_USER" ]] || [[ -z "$POSTGRES_PASSWORD" ]]; then
    log_error "No se pudieron obtener las variables de entorno necesarias del contenedor"
    log_error "POSTGRES_DB: ${POSTGRES_DB:-'NO ENCONTRADA'}"
    log_error "POSTGRES_USER: ${POSTGRES_USER:-'NO ENCONTRADA'}"
    log_error "POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:+'***ENCONTRADA***'}"
    exit 1
fi

log_success "Configuración de BD obtenida correctamente"
log "Base de datos: $POSTGRES_DB"
log "Usuario: $POSTGRES_USER"

# Leer el archivo de consulta
log "Leyendo consulta SQL desde: $QUERY_FILE"
QUERY_CONTENT=$(cat "$QUERY_FILE")

if [[ -z "$QUERY_CONTENT" ]]; then
    log_error "El archivo de consulta está vacío"
    exit 1
fi

# Limpiar comentarios y punto y coma final para evitar errores en \copy
QUERY_CONTENT=$(echo "$QUERY_CONTENT" | grep -v '^[[:space:]]*--' | sed 's/;[[:space:]]*$//' | tr '\n' ' ')

# Preparar comando psql según el formato de salida
case "$OUTPUT_FORMAT" in
    csv)
        PSQL_FORMAT="\\copy ($QUERY_CONTENT) TO STDOUT WITH CSV HEADER"
        ;;
    json)
        # Para JSON usamos una consulta modificada
        PSQL_FORMAT="\\t \\a"
        QUERY_CONTENT="SELECT json_agg(row_to_json(t)) FROM ($QUERY_CONTENT) t;"
        ;;
    table)
        PSQL_FORMAT="\\x auto"
        ;;
    html)
        PSQL_FORMAT="\\H"
        ;;
esac

# Crear archivo temporal con la consulta en el contenedor
TEMP_QUERY_FILE="/tmp/query_$(date +%s).sql"

log "Ejecutando consulta SQL..."

# Ejecutar la consulta
case "$OUTPUT_FORMAT" in
    csv)
        # Para CSV, usar COPY TO STDOUT
        docker exec -i "$DB_CONTAINER" psql \
            -U "$POSTGRES_USER" \
            -d "$POSTGRES_DB" \
            -c "\\copy ($QUERY_CONTENT) TO STDOUT WITH CSV HEADER" 2>/dev/null
        ;;
    json)
        # Para JSON, ejecutar consulta modificada
        docker exec -i "$DB_CONTAINER" psql \
            -U "$POSTGRES_USER" \
            -d "$POSTGRES_DB" \
            -t -A \
            -c "$QUERY_CONTENT" 2>/dev/null
        ;;
    table)
        # Para tabla, usar formato por defecto
        docker exec -i "$DB_CONTAINER" psql \
            -U "$POSTGRES_USER" \
            -d "$POSTGRES_DB" \
            -c "$QUERY_CONTENT" 2>/dev/null
        ;;
    html)
        # Para HTML, usar formato HTML
        docker exec -i "$DB_CONTAINER" psql \
            -U "$POSTGRES_USER" \
            -d "$POSTGRES_DB" \
            -H \
            -c "$QUERY_CONTENT" 2>/dev/null
        ;;
esac

QUERY_EXIT_CODE=$?

# Verificar el resultado
if [[ $QUERY_EXIT_CODE -eq 0 ]]; then
    log_success "Consulta ejecutada correctamente"
else
    log_error "Error al ejecutar la consulta (código de salida: $QUERY_EXIT_CODE)"

    # Intentar obtener más detalles del error
    log_error "Ejecutando consulta para obtener detalles del error..."
    docker exec -i "$DB_CONTAINER" psql \
        -U "$POSTGRES_USER" \
        -d "$POSTGRES_DB" \
        -c "$QUERY_CONTENT" 2>&1 || true

    exit $QUERY_EXIT_CODE
fi

# Información adicional para debugging
if [[ "$QUIET_MODE" != "true" ]]; then
    log "Información de conexión utilizada:"
    log "- Host: localhost (a través del contenedor)"
    log "- Puerto: 5432 (interno del contenedor)"
    log "- Base de datos: $POSTGRES_DB"
    log "- Usuario: $POSTGRES_USER"
    log "- Contenedor: $DB_CONTAINER"
fi

exit 0