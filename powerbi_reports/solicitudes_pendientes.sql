-- =============================================================================
-- CONSULTA: SOLICITUDES PENDIENTES CON TIEMPO DE ESPERA
-- =============================================================================
-- Descripción: Lista todas las solicitudes pendientes con información de validadores
-- Uso: ./execute_query.sh solicitudes_pendientes.sql
-- =============================================================================

SELECT
    pr.id,
    requester.username as requester,
    requester.full_name as requester_name,
    requester.department as requester_department,
    f.path as folder_path,
    f.name as folder_name,
    pr.permission_type,
    pr.justification,
    pr.created_at,
    EXTRACT(DAY FROM (NOW() - pr.created_at)) as days_pending,
    STRING_AGG(DISTINCT owners.username, ', ' ORDER BY owners.username) as possible_validators_owners,
    STRING_AGG(DISTINCT validators.username, ', ' ORDER BY validators.username) as possible_validators_validators
FROM permission_requests pr
JOIN users requester ON pr.requester_id = requester.id
JOIN folders f ON pr.folder_id = f.id
LEFT JOIN folder_owners fo ON f.id = fo.folder_id
LEFT JOIN users owners ON fo.user_id = owners.id AND owners.is_active = true
LEFT JOIN folder_validators fv ON f.id = fv.folder_id
LEFT JOIN users validators ON fv.user_id = validators.id AND validators.is_active = true
WHERE pr.status = 'pending'
GROUP BY pr.id, requester.username, requester.full_name, requester.department,
         f.path, f.name, pr.permission_type, pr.justification, pr.created_at
ORDER BY pr.created_at ASC;