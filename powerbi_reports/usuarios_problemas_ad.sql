-- =============================================================================
-- CONSULTA: USUARIOS CON PROBLEMAS EN ACTIVE DIRECTORY
-- =============================================================================
-- Descripción: Lista usuarios con problemas AD y su nivel de criticidad
-- Incluye tanto problemas reconocidos como no reconocidos
-- Uso: ./execute_query.sh usuarios_problemas_ad.sql
-- =============================================================================

SELECT
    u.id,
    u.username,
    u.full_name,
    COALESCE(u.department, 'Sin departamento') as department,
    CASE
        WHEN u.ad_status = 'not_found' THEN 'No encontrado en AD'
        WHEN u.ad_status = 'disabled' THEN 'Deshabilitado en AD'
        WHEN u.ad_status = 'error' THEN 'Error verificación AD'
        ELSE u.ad_status
    END as ad_status,
    u.ad_last_check,
    u.ad_error_count,
    CASE
        WHEN u.ad_acknowledged = true THEN 'Reconocido'
        ELSE 'No reconocido'
    END as acknowledged,
    u.ad_acknowledged_at as fecha_reconocimiento,
    acknowledged_by.username as reconocido_por,
    CASE
        WHEN owned_folders.count > 0 OR validated_folders.count > 0 THEN 'CRITICO'
        WHEN active_permissions.count > 0 THEN 'ALTO'
        ELSE 'MEDIO'
    END as criticality_level,
    COALESCE(owned_folders.count, 0) as owned_folders,
    COALESCE(validated_folders.count, 0) as validated_folders,
    COALESCE(active_permissions.count, 0) as active_permissions
FROM users u
LEFT JOIN (
    SELECT user_id, COUNT(*) as count
    FROM folder_owners
    GROUP BY user_id
) owned_folders ON u.id = owned_folders.user_id
LEFT JOIN (
    SELECT user_id, COUNT(*) as count
    FROM folder_validators
    GROUP BY user_id
) validated_folders ON u.id = validated_folders.user_id
LEFT JOIN (
    SELECT uagm.user_id, COUNT(*) as count
    FROM user_ad_group_memberships uagm
    JOIN folder_permissions fp ON uagm.ad_group_id = fp.ad_group_id
    WHERE uagm.is_active = true AND fp.is_active = true
    GROUP BY uagm.user_id
) active_permissions ON u.id = active_permissions.user_id
LEFT JOIN users acknowledged_by ON u.ad_acknowledged_by = acknowledged_by.id
WHERE u.ad_status IN ('not_found', 'error', 'disabled')
ORDER BY
    CASE u.ad_status
        WHEN 'not_found' THEN 1
        WHEN 'disabled' THEN 2
        WHEN 'error' THEN 3
    END,
    owned_folders.count DESC NULLS LAST,
    validated_folders.count DESC NULLS LAST;