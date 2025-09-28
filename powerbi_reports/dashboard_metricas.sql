-- =============================================================================
-- CONSULTA: MÉTRICAS PRINCIPALES PARA DASHBOARD
-- =============================================================================
-- Descripción: Obtiene contadores generales del sistema para el dashboard principal
-- Uso: ./execute_query.sh dashboard_metricas.sql
-- =============================================================================

SELECT
    'usuarios_totales' as metric_name,
    COUNT(*) as metric_value,
    COUNT(CASE WHEN is_active = true THEN 1 END) as active_count,
    COUNT(CASE WHEN is_active = false THEN 1 END) as inactive_count
FROM users
UNION ALL
SELECT
    'carpetas_totales' as metric_name,
    COUNT(*) as metric_value,
    COUNT(CASE WHEN is_active = true THEN 1 END) as active_count,
    COUNT(CASE WHEN is_active = false THEN 1 END) as inactive_count
FROM folders
UNION ALL
SELECT
    'grupos_ad_totales' as metric_name,
    COUNT(*) as metric_value,
    COUNT(CASE WHEN is_active = true THEN 1 END) as active_count,
    COUNT(CASE WHEN is_active = false THEN 1 END) as inactive_count
FROM ad_groups
UNION ALL
SELECT
    'solicitudes_pendientes' as metric_name,
    COUNT(*) as metric_value,
    0 as active_count,
    0 as inactive_count
FROM permission_requests
WHERE status = 'pending'