-- =============================================================================
-- CONSULTA: LISTADO DE USUARIOS CON PERMISOS DE CARPETAS
-- =============================================================================
-- Descripción: Lista completa de usuarios con sus permisos de acceso a carpetas
-- Incluye tanto permisos a través de grupos AD como permisos directos
-- Campos: usuario, nombre_completo, departamento, carpeta, tipo_permiso, fecha_permiso
-- Uso: ./execute_query.sh usuarios_permisos_carpetas.sql
-- =============================================================================

SELECT DISTINCT
    u.username as usuario,
    u.full_name as nombre_completo,
    COALESCE(u.department, 'Sin departamento') as departamento,
    COALESCE(f.name, 'Sin nombre') as carpeta,
    fp.permission_type as tipo_permiso,
    fp.granted_at as fecha_permiso,
    granted_by.username as otorgado_por,
    ag.name as grupo_ad,
    'Grupo AD' as fuente_permiso,
    CASE
        WHEN u.ad_status = 'active' THEN 'Activo'
        WHEN u.ad_status = 'not_found' THEN 'No encontrado en AD'
        WHEN u.ad_status = 'disabled' THEN 'Deshabilitado en AD'
        WHEN u.ad_status = 'error' THEN 'Error verificación AD'
        ELSE u.ad_status
    END as estado_ad_usuario,
    CASE
        WHEN ag.ad_status = 'active' THEN 'Activo'
        WHEN ag.ad_status = 'not_found' THEN 'No encontrado en AD'
        WHEN ag.ad_status = 'disabled' THEN 'Deshabilitado en AD'
        WHEN ag.ad_status = 'error' THEN 'Error verificación AD'
        ELSE ag.ad_status
    END as estado_ad_grupo
FROM users u
JOIN user_ad_group_memberships uagm ON u.id = uagm.user_id
JOIN ad_groups ag ON uagm.ad_group_id = ag.id
JOIN folder_permissions fp ON ag.id = fp.ad_group_id
JOIN folders f ON fp.folder_id = f.id
LEFT JOIN users granted_by ON fp.granted_by_id = granted_by.id
WHERE u.is_active = true
  AND uagm.is_active = true
  AND fp.is_active = true
  AND f.is_active = true

UNION ALL

SELECT DISTINCT
    u.username as usuario,
    u.full_name as nombre_completo,
    COALESCE(u.department, 'Sin departamento') as departamento,
    COALESCE(f.name, 'Sin nombre') as carpeta,
    ufp.permission_type as tipo_permiso,
    ufp.granted_at as fecha_permiso,
    granted_by.username as otorgado_por,
    'Asignación directa' as grupo_ad,
    'Permiso directo' as fuente_permiso,
    CASE
        WHEN u.ad_status = 'active' THEN 'Activo'
        WHEN u.ad_status = 'not_found' THEN 'No encontrado en AD'
        WHEN u.ad_status = 'disabled' THEN 'Deshabilitado en AD'
        WHEN u.ad_status = 'error' THEN 'Error verificación AD'
        ELSE u.ad_status
    END as estado_ad_usuario,
    'N/A' as estado_ad_grupo
FROM users u
JOIN user_folder_permissions ufp ON u.id = ufp.user_id
JOIN folders f ON ufp.folder_id = f.id
LEFT JOIN users granted_by ON ufp.granted_by_id = granted_by.id
WHERE u.is_active = true
  AND ufp.is_active = true
  AND f.is_active = true

ORDER BY usuario, carpeta, tipo_permiso