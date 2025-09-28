-- =============================================================================
-- SQL QUERIES FOR POWERBI REPORTS - SAR SYSTEM v3
-- =============================================================================
-- These queries are designed for external execution via Ansible
-- Database connection should be configured in Ansible with proper credentials
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. DASHBOARD PRINCIPAL - Resumen General del Sistema
-- -----------------------------------------------------------------------------
-- Métricas generales para el dashboard principal

-- 1.1 Contadores generales
SELECT
    'usuarios_totales' as metric_name,
    COUNT(*) as metric_value,
    COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_count,
    COUNT(CASE WHEN is_active = 0 THEN 1 END) as inactive_count
FROM users
UNION ALL
SELECT
    'carpetas_totales' as metric_name,
    COUNT(*) as metric_value,
    COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_count,
    COUNT(CASE WHEN is_active = 0 THEN 1 END) as inactive_count
FROM folders
UNION ALL
SELECT
    'grupos_ad_totales' as metric_name,
    COUNT(*) as metric_value,
    COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_count,
    COUNT(CASE WHEN is_active = 0 THEN 1 END) as inactive_count
FROM ad_groups
UNION ALL
SELECT
    'solicitudes_pendientes' as metric_name,
    COUNT(*) as metric_value,
    0 as active_count,
    0 as inactive_count
FROM permission_requests
WHERE status = 'pending';

-- 1.2 Estado de verificación AD
SELECT
    'users' as object_type,
    ad_status,
    COUNT(*) as count,
    COUNT(CASE WHEN ad_acknowledged = 1 THEN 1 END) as acknowledged_count
FROM users
GROUP BY ad_status
UNION ALL
SELECT
    'groups' as object_type,
    ad_status,
    COUNT(*) as count,
    0 as acknowledged_count
FROM ad_groups
GROUP BY ad_status;

-- -----------------------------------------------------------------------------
-- 2. REPORTE DE USUARIOS Y ESTADO AD
-- -----------------------------------------------------------------------------

-- 2.1 Lista completa de usuarios con estado AD y roles
SELECT
    u.id,
    u.username,
    u.email,
    u.full_name,
    u.department,
    u.is_active,
    u.ad_status,
    u.ad_last_check,
    u.ad_error_count,
    u.ad_acknowledged,
    u.ad_acknowledged_at,
    ack_user.username as acknowledged_by,
    u.last_login,
    u.created_at,
    GROUP_CONCAT(r.name ORDER BY r.name SEPARATOR ', ') as roles,
    COUNT(DISTINCT fo.folder_id) as owned_folders_count,
    COUNT(DISTINCT fv.folder_id) as validated_folders_count
FROM users u
LEFT JOIN user_roles ur ON u.id = ur.user_id
LEFT JOIN roles r ON ur.role_id = r.id
LEFT JOIN folder_owners fo ON u.id = fo.user_id
LEFT JOIN folder_validators fv ON u.id = fv.user_id
LEFT JOIN users ack_user ON u.ad_acknowledged_by = ack_user.id
GROUP BY u.id, u.username, u.email, u.full_name, u.department, u.is_active,
         u.ad_status, u.ad_last_check, u.ad_error_count, u.ad_acknowledged,
         u.ad_acknowledged_at, ack_user.username, u.last_login, u.created_at;

-- 2.2 Usuarios con problemas AD críticos
SELECT
    u.id,
    u.username,
    u.full_name,
    u.department,
    u.ad_status,
    u.ad_last_check,
    u.ad_error_count,
    u.ad_acknowledged,
    CASE
        WHEN owned_folders.count > 0 OR validated_folders.count > 0 THEN 'CRITICO'
        WHEN active_permissions.count > 0 THEN 'ALTO'
        ELSE 'MEDIO'
    END as criticality_level,
    owned_folders.count as owned_folders,
    validated_folders.count as validated_folders,
    active_permissions.count as active_permissions
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
    WHERE uagm.is_active = 1 AND fp.is_active = 1
    GROUP BY uagm.user_id
) active_permissions ON u.id = active_permissions.user_id
WHERE u.ad_status IN ('not_found', 'error', 'disabled')
ORDER BY
    CASE u.ad_status
        WHEN 'not_found' THEN 1
        WHEN 'disabled' THEN 2
        WHEN 'error' THEN 3
    END,
    owned_folders.count DESC NULLS LAST,
    validated_folders.count DESC NULLS LAST;

-- -----------------------------------------------------------------------------
-- 3. REPORTE DE CARPETAS Y PERMISOS
-- -----------------------------------------------------------------------------

-- 3.1 Lista de carpetas con propietarios y validadores
SELECT
    f.id,
    f.name,
    f.path,
    f.description,
    f.is_active,
    f.created_at,
    created_by.username as created_by,
    GROUP_CONCAT(DISTINCT owners.username ORDER BY owners.username SEPARATOR ', ') as owners,
    GROUP_CONCAT(DISTINCT validators.username ORDER BY validators.username SEPARATOR ', ') as validators,
    COUNT(DISTINCT fp.id) as total_permissions,
    COUNT(DISTINCT CASE WHEN fp.permission_type = 'read' THEN fp.id END) as read_permissions,
    COUNT(DISTINCT CASE WHEN fp.permission_type = 'write' THEN fp.id END) as write_permissions
FROM folders f
LEFT JOIN users created_by ON f.created_by_id = created_by.id
LEFT JOIN folder_owners fo ON f.id = fo.folder_id
LEFT JOIN users owners ON fo.user_id = owners.id AND owners.is_active = 1
LEFT JOIN folder_validators fv ON f.id = fv.folder_id
LEFT JOIN users validators ON fv.user_id = validators.id AND validators.is_active = 1
LEFT JOIN folder_permissions fp ON f.id = fp.folder_id AND fp.is_active = 1
GROUP BY f.id, f.name, f.path, f.description, f.is_active, f.created_at, created_by.username;

-- 3.2 Detalle de permisos por carpeta
SELECT
    f.id as folder_id,
    f.name as folder_name,
    f.path as folder_path,
    ag.id as ad_group_id,
    ag.name as ad_group_name,
    ag.ad_status as group_ad_status,
    fp.permission_type,
    fp.granted_at,
    granted_by.username as granted_by,
    fp.deletion_in_progress,
    COUNT(DISTINCT uagm.user_id) as users_in_group
FROM folders f
JOIN folder_permissions fp ON f.id = fp.folder_id AND fp.is_active = 1
JOIN ad_groups ag ON fp.ad_group_id = ag.id
LEFT JOIN users granted_by ON fp.granted_by_id = granted_by.id
LEFT JOIN user_ad_group_memberships uagm ON ag.id = uagm.ad_group_id AND uagm.is_active = 1
WHERE f.is_active = 1
GROUP BY f.id, f.name, f.path, ag.id, ag.name, ag.ad_status, fp.permission_type,
         fp.granted_at, granted_by.username, fp.deletion_in_progress
ORDER BY f.path, ag.name, fp.permission_type;

-- -----------------------------------------------------------------------------
-- 4. REPORTE DE SOLICITUDES DE PERMISOS
-- -----------------------------------------------------------------------------

-- 4.1 Histórico completo de solicitudes
SELECT
    pr.id,
    pr.status,
    requester.username as requester,
    requester.full_name as requester_name,
    requester.department as requester_department,
    f.path as folder_path,
    f.name as folder_name,
    ag.name as ad_group_name,
    pr.permission_type,
    pr.justification,
    pr.business_need,
    validator.username as validator,
    pr.validation_comment,
    pr.validation_date,
    pr.expires_at,
    pr.created_at,
    pr.updated_at,
    DATEDIFF(
        COALESCE(pr.validation_date, NOW()),
        pr.created_at
    ) as processing_days
FROM permission_requests pr
JOIN users requester ON pr.requester_id = requester.id
JOIN folders f ON pr.folder_id = f.id
LEFT JOIN ad_groups ag ON pr.ad_group_id = ag.id
LEFT JOIN users validator ON pr.validator_id = validator.id
ORDER BY pr.created_at DESC;

-- 4.2 Solicitudes pendientes con tiempo de espera
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
    DATEDIFF(NOW(), pr.created_at) as days_pending,
    GROUP_CONCAT(DISTINCT owners.username ORDER BY owners.username SEPARATOR ', ') as possible_validators_owners,
    GROUP_CONCAT(DISTINCT validators.username ORDER BY validators.username SEPARATOR ', ') as possible_validators_validators
FROM permission_requests pr
JOIN users requester ON pr.requester_id = requester.id
JOIN folders f ON pr.folder_id = f.id
LEFT JOIN folder_owners fo ON f.id = fo.folder_id
LEFT JOIN users owners ON fo.user_id = owners.id AND owners.is_active = 1
LEFT JOIN folder_validators fv ON f.id = fv.folder_id
LEFT JOIN users validators ON fv.user_id = validators.id AND validators.is_active = 1
WHERE pr.status = 'pending'
GROUP BY pr.id, requester.username, requester.full_name, requester.department,
         f.path, f.name, pr.permission_type, pr.justification, pr.created_at
ORDER BY pr.created_at ASC;

-- -----------------------------------------------------------------------------
-- 5. REPORTE DE GRUPOS AD Y MEMBRESÍAS
-- -----------------------------------------------------------------------------

-- 5.1 Estado de grupos AD con impacto
SELECT
    ag.id,
    ag.name,
    ag.distinguished_name,
    ag.group_type,
    ag.is_active,
    ag.ad_status,
    ag.ad_last_check,
    ag.ad_error_count,
    COUNT(DISTINCT fp.id) as folder_permissions_count,
    COUNT(DISTINCT fp.folder_id) as folders_with_permissions,
    COUNT(DISTINCT uagm.user_id) as active_members,
    GROUP_CONCAT(DISTINCT f.path ORDER BY f.path SEPARATOR '; ') as affected_folders
FROM ad_groups ag
LEFT JOIN folder_permissions fp ON ag.id = fp.ad_group_id AND fp.is_active = 1
LEFT JOIN folders f ON fp.folder_id = f.id AND f.is_active = 1
LEFT JOIN user_ad_group_memberships uagm ON ag.id = uagm.ad_group_id AND uagm.is_active = 1
GROUP BY ag.id, ag.name, ag.distinguished_name, ag.group_type, ag.is_active,
         ag.ad_status, ag.ad_last_check, ag.ad_error_count
ORDER BY folder_permissions_count DESC, ag.name;

-- 5.2 Membresías de usuarios en grupos AD
SELECT
    u.username,
    u.full_name,
    u.department,
    u.is_active as user_active,
    ag.name as ad_group_name,
    ag.ad_status as group_ad_status,
    uagm.is_active as membership_active,
    uagm.synchronized_at,
    COUNT(DISTINCT fp.folder_id) as folders_accessible
FROM user_ad_group_memberships uagm
JOIN users u ON uagm.user_id = u.id
JOIN ad_groups ag ON uagm.ad_group_id = ag.id
LEFT JOIN folder_permissions fp ON ag.id = fp.ad_group_id AND fp.is_active = 1
GROUP BY u.username, u.full_name, u.department, u.is_active,
         ag.name, ag.ad_status, uagm.is_active, uagm.synchronized_at
ORDER BY u.username, ag.name;

-- -----------------------------------------------------------------------------
-- 6. REPORTE DE AUDITORÍA
-- -----------------------------------------------------------------------------

-- 6.1 Eventos de auditoría recientes (últimos 30 días)
SELECT
    ae.id,
    ae.event_type,
    ae.action,
    ae.resource_type,
    ae.resource_id,
    ae.description,
    u.username as user_performed,
    u.full_name as user_name,
    ae.ip_address,
    ae.created_at,
    DATE(ae.created_at) as event_date
FROM audit_events ae
LEFT JOIN users u ON ae.user_id = u.id
WHERE ae.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
ORDER BY ae.created_at DESC;

-- 6.2 Estadísticas de actividad por tipo de evento
SELECT
    ae.event_type,
    ae.action,
    COUNT(*) as event_count,
    COUNT(DISTINCT ae.user_id) as unique_users,
    COUNT(DISTINCT DATE(ae.created_at)) as days_with_activity,
    MIN(ae.created_at) as first_event,
    MAX(ae.created_at) as last_event
FROM audit_events ae
WHERE ae.created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
GROUP BY ae.event_type, ae.action
ORDER BY event_count DESC;

-- -----------------------------------------------------------------------------
-- 7. REPORTES DE MÉTRICAS Y KPIs
-- -----------------------------------------------------------------------------

-- 7.1 Tiempo promedio de aprobación de solicitudes
SELECT
    DATE_FORMAT(pr.created_at, '%Y-%m') as month_year,
    pr.status,
    COUNT(*) as total_requests,
    AVG(DATEDIFF(pr.validation_date, pr.created_at)) as avg_processing_days,
    MIN(DATEDIFF(pr.validation_date, pr.created_at)) as min_processing_days,
    MAX(DATEDIFF(pr.validation_date, pr.created_at)) as max_processing_days,
    STDDEV(DATEDIFF(pr.validation_date, pr.created_at)) as stddev_processing_days
FROM permission_requests pr
WHERE pr.validation_date IS NOT NULL
  AND pr.created_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
GROUP BY DATE_FORMAT(pr.created_at, '%Y-%m'), pr.status
ORDER BY month_year DESC, pr.status;

-- 7.2 Top carpetas por número de solicitudes
SELECT
    f.path as folder_path,
    f.name as folder_name,
    COUNT(*) as total_requests,
    COUNT(CASE WHEN pr.status = 'pending' THEN 1 END) as pending_requests,
    COUNT(CASE WHEN pr.status = 'approved' THEN 1 END) as approved_requests,
    COUNT(CASE WHEN pr.status = 'rejected' THEN 1 END) as rejected_requests,
    COUNT(DISTINCT pr.requester_id) as unique_requesters,
    MAX(pr.created_at) as last_request_date
FROM permission_requests pr
JOIN folders f ON pr.folder_id = f.id
WHERE pr.created_at >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
GROUP BY f.path, f.name
HAVING total_requests > 0
ORDER BY total_requests DESC
LIMIT 50;

-- 7.3 Usuarios más activos (solicitudes)
SELECT
    u.username,
    u.full_name,
    u.department,
    COUNT(*) as total_requests,
    COUNT(CASE WHEN pr.status = 'pending' THEN 1 END) as pending_requests,
    COUNT(CASE WHEN pr.status = 'approved' THEN 1 END) as approved_requests,
    COUNT(CASE WHEN pr.status = 'rejected' THEN 1 END) as rejected_requests,
    COUNT(DISTINCT pr.folder_id) as unique_folders_requested,
    MAX(pr.created_at) as last_request_date
FROM permission_requests pr
JOIN users u ON pr.requester_id = u.id
WHERE pr.created_at >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
GROUP BY u.username, u.full_name, u.department
HAVING total_requests > 0
ORDER BY total_requests DESC
LIMIT 50;

-- -----------------------------------------------------------------------------
-- 8. REPORTE DE PROBLEMAS Y ALERTAS
-- -----------------------------------------------------------------------------

-- 8.1 Carpetas sin propietarios o validadores
SELECT
    f.id,
    f.name,
    f.path,
    f.is_active,
    f.created_at,
    CASE
        WHEN owners.count IS NULL AND validators.count IS NULL THEN 'SIN_PROPIETARIOS_NI_VALIDADORES'
        WHEN owners.count IS NULL THEN 'SIN_PROPIETARIOS'
        WHEN validators.count IS NULL THEN 'SIN_VALIDADORES'
        ELSE 'OK'
    END as problem_type,
    COALESCE(owners.count, 0) as owners_count,
    COALESCE(validators.count, 0) as validators_count,
    COALESCE(pending_requests.count, 0) as pending_requests_count
FROM folders f
LEFT JOIN (
    SELECT folder_id, COUNT(*) as count
    FROM folder_owners fo
    JOIN users u ON fo.user_id = u.id AND u.is_active = 1
    GROUP BY folder_id
) owners ON f.id = owners.folder_id
LEFT JOIN (
    SELECT folder_id, COUNT(*) as count
    FROM folder_validators fv
    JOIN users u ON fv.user_id = u.id AND u.is_active = 1
    GROUP BY folder_id
) validators ON f.id = validators.folder_id
LEFT JOIN (
    SELECT folder_id, COUNT(*) as count
    FROM permission_requests
    WHERE status = 'pending'
    GROUP BY folder_id
) pending_requests ON f.id = pending_requests.folder_id
WHERE f.is_active = 1
  AND (owners.count IS NULL OR validators.count IS NULL)
ORDER BY pending_requests_count DESC NULLS LAST, f.created_at DESC;

-- 8.2 Solicitudes pendientes por mucho tiempo
SELECT
    pr.id,
    pr.created_at,
    DATEDIFF(NOW(), pr.created_at) as days_pending,
    requester.username as requester,
    f.path as folder_path,
    pr.permission_type,
    pr.justification,
    CASE
        WHEN DATEDIFF(NOW(), pr.created_at) > 30 THEN 'CRITICO'
        WHEN DATEDIFF(NOW(), pr.created_at) > 14 THEN 'ALTO'
        WHEN DATEDIFF(NOW(), pr.created_at) > 7 THEN 'MEDIO'
        ELSE 'NORMAL'
    END as priority_level
FROM permission_requests pr
JOIN users requester ON pr.requester_id = requester.id
JOIN folders f ON pr.folder_id = f.id
WHERE pr.status = 'pending'
  AND DATEDIFF(NOW(), pr.created_at) > 7
ORDER BY days_pending DESC;

-- -----------------------------------------------------------------------------
-- 9. CONSULTAS PARA ANÁLISIS DE TENDENCIAS
-- -----------------------------------------------------------------------------

-- 9.1 Evolución de solicitudes por mes
SELECT
    DATE_FORMAT(created_at, '%Y-%m') as month_year,
    COUNT(*) as total_requests,
    COUNT(CASE WHEN status = 'approved' THEN 1 END) as approved,
    COUNT(CASE WHEN status = 'rejected' THEN 1 END) as rejected,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
    ROUND(COUNT(CASE WHEN status = 'approved' THEN 1 END) * 100.0 / COUNT(*), 2) as approval_rate
FROM permission_requests
WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 MONTH)
GROUP BY DATE_FORMAT(created_at, '%Y-%m')
ORDER BY month_year;

-- 9.2 Distribución por departamentos
SELECT
    u.department,
    COUNT(DISTINCT u.id) as total_users,
    COUNT(DISTINCT pr.id) as total_requests,
    COUNT(DISTINCT CASE WHEN pr.status = 'approved' THEN pr.id END) as approved_requests,
    ROUND(COUNT(DISTINCT pr.id) * 1.0 / COUNT(DISTINCT u.id), 2) as requests_per_user,
    COUNT(DISTINCT fo.folder_id) as folders_owned,
    COUNT(DISTINCT fv.folder_id) as folders_validated
FROM users u
LEFT JOIN permission_requests pr ON u.id = pr.requester_id
    AND pr.created_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
LEFT JOIN folder_owners fo ON u.id = fo.user_id
LEFT JOIN folder_validators fv ON u.id = fv.user_id
WHERE u.department IS NOT NULL AND u.department != ''
GROUP BY u.department
ORDER BY total_requests DESC;

-- -----------------------------------------------------------------------------
-- 10. CONSULTA PARA VERIFICACIÓN DE INTEGRIDAD
-- -----------------------------------------------------------------------------

-- 10.1 Resumen de integridad del sistema
SELECT
    'Usuarios con problemas AD no reconocidos' as check_name,
    COUNT(*) as count,
    'CRITICAL' as severity
FROM users
WHERE ad_status IN ('not_found', 'error', 'disabled')
  AND ad_acknowledged = 0

UNION ALL

SELECT
    'Grupos AD con problemas y permisos activos' as check_name,
    COUNT(*) as count,
    'HIGH' as severity
FROM ad_groups ag
JOIN folder_permissions fp ON ag.id = fp.ad_group_id AND fp.is_active = 1
WHERE ag.ad_status IN ('not_found', 'error', 'disabled')

UNION ALL

SELECT
    'Solicitudes pendientes > 30 días' as check_name,
    COUNT(*) as count,
    'MEDIUM' as severity
FROM permission_requests
WHERE status = 'pending'
  AND DATEDIFF(NOW(), created_at) > 30

UNION ALL

SELECT
    'Carpetas activas sin propietarios' as check_name,
    COUNT(*) as count,
    'HIGH' as severity
FROM folders f
LEFT JOIN folder_owners fo ON f.id = fo.folder_id
LEFT JOIN users u ON fo.user_id = u.id AND u.is_active = 1
WHERE f.is_active = 1
  AND u.id IS NULL

ORDER BY
    CASE severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END;