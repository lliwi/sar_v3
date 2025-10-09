-- =============================================================================
-- CONSULTA: HISTORIAL COMPLETO DE AUDITORÍA
-- =============================================================================
-- Descripción: Registro completo de eventos de auditoría del sistema
-- Incluye todos los tipos de eventos: login, solicitudes, cambios de permisos, etc.
-- Campos: id_evento, fecha, usuario, nombre_usuario, tipo_evento, accion,
--         tipo_recurso, id_recurso, descripcion, ip_address
-- Uso: ./execute_query.sh auditoria_eventos.sql
-- =============================================================================

SELECT
    ae.id as id_evento,
    ae.created_at as fecha,
    COALESCE(u.username, 'Sistema') as usuario,
    CASE
        WHEN u.full_name LIKE '%,%' THEN
            TRIM(SUBSTRING(u.full_name FROM POSITION(',' IN u.full_name) + 1)) || ' ' ||
            TRIM(SUBSTRING(u.full_name FROM 1 FOR POSITION(',' IN u.full_name) - 1))
        WHEN u.full_name IS NOT NULL THEN u.full_name
        ELSE 'Sistema'
    END as nombre_usuario,
    u.department as departamento,
    CASE
        WHEN u.email LIKE '%partner.italdesign%' THEN 'EXT'
        WHEN u.email IS NOT NULL THEN 'INT'
        ELSE 'SISTEMA'
    END as tipo_usuario,
    CASE
        WHEN ae.event_type = 'login' THEN 'Inicio de Sesión'
        WHEN ae.event_type = 'permission_request' THEN 'Solicitud de Permiso'
        WHEN ae.event_type = 'permission_granted' THEN 'Permiso Otorgado'
        WHEN ae.event_type = 'permission_revoked' THEN 'Permiso Revocado'
        WHEN ae.event_type = 'folder_access' THEN 'Acceso a Carpeta'
        WHEN ae.event_type = 'user_management' THEN 'Gestión de Usuario'
        WHEN ae.event_type = 'folder_management' THEN 'Gestión de Carpeta'
        WHEN ae.event_type = 'ad_sync' THEN 'Sincronización AD'
        WHEN ae.event_type = 'task_execution' THEN 'Ejecución de Tarea'
        WHEN ae.event_type = 'validation' THEN 'Validación'
        ELSE UPPER(ae.event_type)
    END as tipo_evento,
    CASE
        WHEN ae.action = 'create' THEN 'CREAR'
        WHEN ae.action = 'update' THEN 'ACTUALIZAR'
        WHEN ae.action = 'delete' THEN 'ELIMINAR'
        WHEN ae.action = 'approve' THEN 'APROBAR'
        WHEN ae.action = 'reject' THEN 'RECHAZAR'
        WHEN ae.action = 'login' THEN 'INICIO SESIÓN'
        WHEN ae.action = 'logout' THEN 'CIERRE SESIÓN'
        WHEN ae.action = 'view' THEN 'VISUALIZAR'
        WHEN ae.action = 'export' THEN 'EXPORTAR'
        WHEN ae.action = 'sync' THEN 'SINCRONIZAR'
        WHEN ae.action = 'failed' THEN 'FALLIDO'
        WHEN ae.action = 'auto_failed' THEN 'FALLO AUTOMÁTICO'
        WHEN ae.action = 'cancelled' THEN 'CANCELADO'
        ELSE UPPER(ae.action)
    END as accion,
    CASE
        WHEN ae.resource_type = 'folder' THEN 'Carpeta'
        WHEN ae.resource_type = 'user' THEN 'Usuario'
        WHEN ae.resource_type = 'permission' THEN 'Permiso'
        WHEN ae.resource_type = 'permission_request' THEN 'Solicitud de Permiso'
        WHEN ae.resource_type = 'ad_group' THEN 'Grupo AD'
        WHEN ae.resource_type = 'task' THEN 'Tarea'
        WHEN ae.resource_type = 'role' THEN 'Rol'
        ELSE UPPER(ae.resource_type)
    END as tipo_recurso,
    ae.resource_id as id_recurso,
    ae.description as descripcion,
    ae.ip_address as direccion_ip,
    ae.user_agent as navegador,
    -- Información adicional si el evento está relacionado con carpetas
    CASE
        WHEN ae.resource_type = 'folder' THEN
            (SELECT f.name FROM folders f WHERE f.id = ae.resource_id)
        ELSE NULL
    END as carpeta_afectada,
    CASE
        WHEN ae.resource_type = 'folder' THEN
            (SELECT f.path FROM folders f WHERE f.id = ae.resource_id)
        ELSE NULL
    END as carpeta_path,
    -- Información adicional si el evento está relacionado con solicitudes
    CASE
        WHEN ae.resource_type = 'permission_request' THEN
            (SELECT pr.status FROM permission_requests pr WHERE pr.id = ae.resource_id)
        ELSE NULL
    END as estado_solicitud
FROM audit_events ae
LEFT JOIN users u ON ae.user_id = u.id
ORDER BY ae.created_at DESC
