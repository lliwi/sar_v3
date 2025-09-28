-- =============================================================================
-- CONSULTA: ESTADO ACTUAL DE USUARIOS DEL SISTEMA
-- =============================================================================
-- Descripción: Lista de usuarios con su información básica y estado en AD
-- Útil para auditoría de usuarios antes de asignar permisos
-- Uso: ./execute_query.sh usuarios_estado_actual.sql
-- =============================================================================

SELECT
    u.username as usuario,
    u.full_name as nombre_completo,
    COALESCE(u.department, 'Sin departamento') as departamento,
    u.email as email,
    CASE
        WHEN u.ad_status = 'active' THEN 'Activo'
        WHEN u.ad_status = 'not_found' THEN 'No encontrado en AD'
        WHEN u.ad_status = 'disabled' THEN 'Deshabilitado en AD'
        WHEN u.ad_status = 'error' THEN 'Error verificación AD'
        ELSE u.ad_status
    END as estado_ad,
    u.is_active as usuario_activo,
    u.last_login as ultimo_login,
    u.created_at as fecha_creacion,
    'Sin permisos asignados aún' as estado_permisos
FROM users u
WHERE u.is_active = true
ORDER BY u.username
LIMIT 50