-- =============================================================================
-- CONSULTA: RESUMEN DE SOLICITUDES POR ESTADO
-- =============================================================================
-- Descripción: Resumen estadístico de solicitudes para dashboard
-- Uso: ./execute_query.sh resumen_solicitudes.sql
-- =============================================================================

SELECT
    CASE
        WHEN pr.status = 'pending' THEN 'Pendientes'
        WHEN pr.status = 'approved' THEN 'Aprobadas'
        WHEN pr.status = 'rejected' THEN 'Rechazadas'
        WHEN pr.status = 'canceled' THEN 'Canceladas'
        WHEN pr.status = 'failed' THEN 'Fallidas'
        WHEN pr.status = 'revoked' THEN 'Revocadas'
        ELSE 'Otros'
    END as estado_solicitud,
    COUNT(*) as total_solicitudes,
    COUNT(CASE WHEN pr.permission_type = 'read' THEN 1 END) as permisos_lectura,
    COUNT(CASE WHEN pr.permission_type = 'write' THEN 1 END) as permisos_escritura,
    AVG(
        CASE
            WHEN pr.validation_date IS NOT NULL THEN
                EXTRACT(DAY FROM (pr.validation_date - pr.created_at))
            ELSE NULL
        END
    ) as dias_promedio_procesamiento,
    MIN(pr.created_at) as primera_solicitud,
    MAX(pr.created_at) as ultima_solicitud,
    COUNT(DISTINCT pr.requester_id) as usuarios_solicitantes,
    COUNT(DISTINCT pr.folder_id) as carpetas_solicitadas
FROM permission_requests pr
GROUP BY pr.status
ORDER BY total_solicitudes DESC