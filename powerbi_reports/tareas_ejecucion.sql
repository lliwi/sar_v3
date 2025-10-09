-- =============================================================================
-- CONSULTA: HISTORIAL DE TAREAS DE EJECUCIÓN
-- =============================================================================
-- Descripción: Detalle completo de todas las tareas ejecutadas en el sistema
-- Incluye tareas de Airflow DAG y verificaciones AD
-- Campos: id_tarea, nombre_tarea, tipo_tarea, estado, intentos, solicitud_id,
--         creado_por, fecha_creacion, fecha_inicio, fecha_finalizacion,
--         tiempo_ejecucion_minutos, mensaje_error
-- Uso: ./execute_query.sh tareas_ejecucion.sql
-- =============================================================================

SELECT
    t.id as id_tarea,
    t.name as nombre_tarea,
    CASE
        WHEN t.task_type = 'airflow_dag' THEN 'DAG Airflow'
        WHEN t.task_type = 'ad_verification' THEN 'Verificación AD'
        ELSE UPPER(t.task_type)
    END as tipo_tarea,
    CASE
        WHEN t.status = 'pending' THEN 'PENDIENTE'
        WHEN t.status = 'running' THEN 'EJECUTANDO'
        WHEN t.status = 'completed' THEN 'COMPLETADA'
        WHEN t.status = 'failed' THEN 'FALLIDA'
        WHEN t.status = 'retry' THEN 'REINTENTANDO'
        WHEN t.status = 'cancelled' THEN 'CANCELADA'
        ELSE UPPER(t.status)
    END as estado,
    t.attempt_count as intentos,
    t.max_attempts as max_intentos,
    t.permission_request_id as solicitud_id,
    CASE
        WHEN u.full_name LIKE '%,%' THEN
            TRIM(SUBSTRING(u.full_name FROM POSITION(',' IN u.full_name) + 1)) || ' ' ||
            TRIM(SUBSTRING(u.full_name FROM 1 FOR POSITION(',' IN u.full_name) - 1))
        ELSE u.full_name
    END as creado_por,
    u.username as usuario_creador,
    t.created_at as fecha_creacion,
    t.started_at as fecha_inicio,
    t.completed_at as fecha_finalizacion,
    CASE
        WHEN t.started_at IS NOT NULL AND t.completed_at IS NOT NULL THEN
            EXTRACT(EPOCH FROM (t.completed_at - t.started_at)) / 60.0
        ELSE NULL
    END as tiempo_ejecucion_minutos,
    t.error_message as mensaje_error,
    -- Datos relacionados con la solicitud de permiso (si existe)
    pr.id as solicitud_permiso_id,
    f.name as carpeta_nombre,
    f.path as carpeta_path,
    CASE
        WHEN pr.permission_type = 'read' THEN 'LECTURA'
        WHEN pr.permission_type = 'write' THEN 'ESCRITURA'
        ELSE UPPER(pr.permission_type)
    END as tipo_permiso,
    ag.name as grupo_ad,
    CASE
        WHEN req_user.full_name LIKE '%,%' THEN
            TRIM(SUBSTRING(req_user.full_name FROM POSITION(',' IN req_user.full_name) + 1)) || ' ' ||
            TRIM(SUBSTRING(req_user.full_name FROM 1 FOR POSITION(',' IN req_user.full_name) - 1))
        ELSE req_user.full_name
    END as solicitante
FROM tasks t
LEFT JOIN users u ON t.created_by_id = u.id
LEFT JOIN permission_requests pr ON t.permission_request_id = pr.id
LEFT JOIN folders f ON pr.folder_id = f.id
LEFT JOIN ad_groups ag ON pr.ad_group_id = ag.id
LEFT JOIN users req_user ON pr.requester_id = req_user.id
ORDER BY t.created_at DESC
