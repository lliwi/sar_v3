-- =============================================================================
-- CONSULTA: DETALLE DE USUARIOS Y PERMISOS DE CARPETAS
-- =============================================================================
-- Descripción: Lista simplificada de usuarios con sus permisos de carpetas
-- Incluye clasificación de usuarios (INT/EXT) y normalización de permisos
-- Campos: nombre_completo, tipo_usuario, nombre_carpeta, path, tipo_permiso, fecha
-- Uso: ./execute_query.sh usuarios_permisos_detalle.sql
-- =============================================================================

SELECT DISTINCT
    CASE
        WHEN u.full_name LIKE '%,%' THEN
            TRIM(SUBSTRING(u.full_name FROM POSITION(',' IN u.full_name) + 1)) || ' ' ||
            TRIM(SUBSTRING(u.full_name FROM 1 FOR POSITION(',' IN u.full_name) - 1))
        ELSE u.full_name
    END as nombre_completo,
    CASE
        WHEN u.email LIKE '%partner.italdesign%' THEN 'EXT'
        ELSE 'INT'
    END as tipo_usuario,
    f.name as nombre_carpeta,
    f.path as path,
    CASE
        WHEN fp.permission_type = 'read' THEN 'LECTURA'
        WHEN fp.permission_type = 'write' THEN 'ESCRITURA'
        ELSE UPPER(fp.permission_type)
    END as tipo_permiso,
    fp.granted_at as fecha
FROM users u
JOIN user_ad_group_memberships uagm ON u.id = uagm.user_id
JOIN ad_groups ag ON uagm.ad_group_id = ag.id
JOIN folder_permissions fp ON ag.id = fp.ad_group_id
JOIN folders f ON fp.folder_id = f.id
WHERE u.is_active = true
  AND uagm.is_active = true
  AND fp.is_active = true
  AND f.is_active = true
  AND u.department = 'IG/EK-B'

UNION ALL

SELECT DISTINCT
    CASE
        WHEN u.full_name LIKE '%,%' THEN
            TRIM(SUBSTRING(u.full_name FROM POSITION(',' IN u.full_name) + 1)) || ' ' ||
            TRIM(SUBSTRING(u.full_name FROM 1 FOR POSITION(',' IN u.full_name) - 1))
        ELSE u.full_name
    END as nombre_completo,
    CASE
        WHEN u.email LIKE '%partner.italdesign%' THEN 'EXT'
        ELSE 'INT'
    END as tipo_usuario,
    f.name as nombre_carpeta,
    f.path as path,
    CASE
        WHEN ufp.permission_type = 'read' THEN 'LECTURA'
        WHEN ufp.permission_type = 'write' THEN 'ESCRITURA'
        ELSE UPPER(ufp.permission_type)
    END as tipo_permiso,
    ufp.granted_at as fecha
FROM users u
JOIN user_folder_permissions ufp ON u.id = ufp.user_id
JOIN folders f ON ufp.folder_id = f.id
WHERE u.is_active = true
  AND ufp.is_active = true
  AND f.is_active = true
  AND u.department = 'IG/EK-B'

ORDER BY nombre_completo, nombre_carpeta, tipo_permiso
