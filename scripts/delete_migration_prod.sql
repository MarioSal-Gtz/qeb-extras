-- ============================================================
-- DELETE MIGRATED DATA FROM PRODUCTION
-- Only deletes data with "Migración" in solicitud.notas
-- Does NOT touch user-created data
-- Run in MySQL Workbench connected to DigitalOcean production
-- ============================================================

USE u658050396_QEB;
SET FOREIGN_KEY_CHECKS = 0;
SET sql_require_primary_key = 0;

-- 1. Delete reservas linked to migrated solicitudCaras
DELETE r FROM reservas r
INNER JOIN solicitudCaras sc ON r.solicitudCaras_id = sc.id
INNER JOIN propuesta p ON p.id = CAST(sc.idquote AS UNSIGNED)
INNER JOIN solicitud s ON s.id = p.solicitud_id
WHERE s.notas LIKE '%Migración%';

-- 2. Delete solicitudCaras linked to migrated propuestas
DELETE sc FROM solicitudCaras sc
INNER JOIN propuesta p ON p.id = CAST(sc.idquote AS UNSIGNED)
INNER JOIN solicitud s ON s.id = p.solicitud_id
WHERE s.notas LIKE '%Migración%';

-- 3. Delete campañas linked to migrated cotizaciones
DELETE cm FROM campania cm
INNER JOIN cotizacion ct ON cm.cotizacion_id = ct.id
WHERE ct.observaciones LIKE '%Migración%';

-- 4. Delete cotizaciones migradas
DELETE FROM cotizacion WHERE observaciones LIKE '%Migración%';

-- 5. Delete historial of migrated solicitudes
DELETE h FROM historial h
INNER JOIN solicitud s ON h.ref_id = s.id AND h.tipo = 'Solicitud'
WHERE s.notas LIKE '%Migración%';

-- 6. Delete propuestas of migrated solicitudes
DELETE p FROM propuesta p
INNER JOIN solicitud s ON p.solicitud_id = s.id
WHERE s.notas LIKE '%Migración%';

-- 7. Delete migrated solicitudes
DELETE FROM solicitud WHERE notas LIKE '%Migración%';

SET FOREIGN_KEY_CHECKS = 1;

-- Verify
SELECT 'Solicitudes migradas restantes' as check_name, COUNT(*) as count FROM solicitud WHERE notas LIKE '%Migración%'
UNION ALL
SELECT 'Cotizaciones migradas restantes', COUNT(*) FROM cotizacion WHERE observaciones LIKE '%Migración%'
UNION ALL
SELECT 'Solicitudes usuarios (no tocar)', COUNT(*) FROM solicitud WHERE notas NOT LIKE '%Migración%' OR notas IS NULL;
