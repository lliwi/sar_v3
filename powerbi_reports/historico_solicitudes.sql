-- =============================================================================
-- CONSULTA: HISTÓRICO COMPLETO DE SOLICITUDES DE PERMISOS
-- =============================================================================
-- Descripción: Lista todas las solicitudes realizadas con información del solicitante y aprobador
-- Incluye información completa para auditoría y análisis de procesos de aprobación
-- Campos: solicitante_usuario, solicitante_nombre, aprobador_usuario, aprobador_nombre, carpeta, tipo_permiso, fecha_solicitud, fecha_aprobacion, estado
-- Uso: ./execute_query.sh historico_solicitudes.sql
-- =============================================================================

SELECT
    pr.id as solicitud_id,
    requester.username as solicitante_usuario,
    requester.full_name as solicitante_nombre,
    COALESCE(requester.department, 'Sin departamento') as solicitante_departamento,
    COALESCE(validator.username, 'No asignado') as aprobador_usuario,
    COALESCE(validator.full_name, 'No asignado') as aprobador_nombre,
    COALESCE(f.name, 'Sin nombre') as carpeta,
    CASE
        WHEN pr.permission_type = 'read' THEN 'Lectura'
        WHEN pr.permission_type = 'write' THEN 'Escritura'
        ELSE pr.permission_type
    END as tipo_permiso,
    pr.created_at as fecha_solicitud,
    pr.validation_date as fecha_aprobacion,
    CASE
        WHEN pr.status = 'pending' THEN 'Pendiente'
        WHEN pr.status = 'approved' THEN 'Aprobada'
        WHEN pr.status = 'rejected' THEN 'Rechazada'
        WHEN pr.status = 'canceled' THEN 'Cancelada'
        WHEN pr.status = 'failed' THEN 'Fallida'
        WHEN pr.status = 'revoked' THEN 'Revocada'
        ELSE UPPER(SUBSTRING(pr.status, 1, 1)) || LOWER(SUBSTRING(pr.status, 2))
    END as estado,
    COALESCE(pr.justification, 'Sin justificación') as justificacion,
    COALESCE(pr.business_need, 'Sin especificar') as necesidad_negocio,
    COALESCE(pr.validation_comment, 'Sin comentarios') as comentario_validacion,
    CASE
        WHEN pr.validation_date IS NOT NULL THEN
            EXTRACT(DAY FROM (pr.validation_date - pr.created_at))
        ELSE
            EXTRACT(DAY FROM (NOW() - pr.created_at))
    END as dias_procesamiento,
    COALESCE(ag.name, 'Sin grupo asignado') as grupo_ad_asignado,
    pr.expires_at as fecha_expiracion,
    CASE
        WHEN pr.expires_at IS NOT NULL AND pr.expires_at < NOW() THEN 'Expirado'
        WHEN pr.expires_at IS NOT NULL THEN 'Con expiración'
        ELSE 'Sin expiración'
    END as estado_expiracion
FROM permission_requests pr
JOIN users requester ON pr.requester_id = requester.id
JOIN folders f ON pr.folder_id = f.id
LEFT JOIN users validator ON pr.validator_id = validator.id
LEFT JOIN ad_groups ag ON pr.ad_group_id = ag.id
ORDER BY pr.created_at DESC