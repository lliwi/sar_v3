-- =============================================================================
-- CONSULTA: PROPIETARIOS DE CARPETAS CON CONTADORES DE USUARIOS
-- =============================================================================
-- Descripción: Lista propietarios con sus carpetas y contadores de usuarios por tipo de permiso
-- Incluye separación entre usuarios con permisos de lectura y escritura
-- Campos: propietario_usuario, propietario_nombre, departamento, nombre_carpeta, usuarios_lectura, usuarios_escritura
-- Uso: ./execute_query.sh propietarios_carpetas.sql
-- =============================================================================

SELECT
    u.username as propietario_usuario,
    u.full_name as propietario_nombre,
    COALESCE(u.department, 'Sin departamento') as departamento,
    f.name as nombre_carpeta,
    COALESCE(read_users.count, 0) as usuarios_lectura,
    COALESCE(write_users.count, 0) as usuarios_escritura,
    COALESCE(read_users.count, 0) + COALESCE(write_users.count, 0) as total_usuarios
FROM users u
JOIN folder_owners fo ON u.id = fo.user_id
JOIN folders f ON fo.folder_id = f.id
LEFT JOIN (
    SELECT
        fp.folder_id,
        COUNT(DISTINCT uagm.user_id) as count
    FROM folder_permissions fp
    JOIN user_ad_group_memberships uagm ON fp.ad_group_id = uagm.ad_group_id
    WHERE fp.permission_type = 'read'
      AND fp.is_active = true
      AND uagm.is_active = true
    GROUP BY fp.folder_id
) read_users ON f.id = read_users.folder_id
LEFT JOIN (
    SELECT
        fp.folder_id,
        COUNT(DISTINCT uagm.user_id) as count
    FROM folder_permissions fp
    JOIN user_ad_group_memberships uagm ON fp.ad_group_id = uagm.ad_group_id
    WHERE fp.permission_type = 'write'
      AND fp.is_active = true
      AND uagm.is_active = true
    GROUP BY fp.folder_id
) write_users ON f.id = write_users.folder_id
ORDER BY
    u.username,
    f.name