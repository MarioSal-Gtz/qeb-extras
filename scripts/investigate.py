import mysql.connector

conn = mysql.connector.connect(
    host='srv1978.hstgr.io', port=3306,
    user='u658050396_QEB', password='QEBdevelop.1',
    database='u658050396_QEB', charset='utf8mb4'
)
cursor = conn.cursor(dictionary=True)

# 1. All propuestas
print('=== TODAS LAS PROPUESTAS ===')
cursor.execute('SELECT id, status, deleted_at, solicitud_id, cliente_id, fecha FROM propuesta ORDER BY id')
for r in cursor.fetchall():
    print(f'  id={r["id"]} | status={r["status"]} | deleted={r["deleted_at"]} | sol={r["solicitud_id"]} | fecha={r["fecha"]}')

# 2. All solicitudes
print('\n=== SOLICITUDES ===')
cursor.execute('SELECT id, descripcion, status, deleted_at FROM solicitud ORDER BY id')
for r in cursor.fetchall():
    print(f'  id={r["id"]} | {r["descripcion"][:40]} | status={r["status"]} | deleted={r["deleted_at"]}')

# 3. All campanias
print('\n=== CAMPANIAS ===')
cursor.execute('SELECT id, nombre, status, cotizacion_id, total_caras FROM campania ORDER BY id')
for r in cursor.fetchall():
    print(f'  id={r["id"]} | {r["nombre"][:30]} | status={r["status"]} | cot={r["cotizacion_id"]} | caras={r["total_caras"]}')

# 4. Cotizaciones
print('\n=== COTIZACIONES ===')
cursor.execute('SELECT id, id_propuesta, nombre_campania, status FROM cotizacion ORDER BY id')
for r in cursor.fetchall():
    print(f'  id={r["id"]} | propuesta={r["id_propuesta"]} | {r["nombre_campania"][:30]} | status={r["status"]}')

# 5. Reservas por propuesta
print('\n=== RESERVAS POR PROPUESTA ===')
cursor.execute('''
    SELECT sc.idquote as propuesta_id,
           COUNT(*) as total,
           SUM(CASE WHEN r.deleted_at IS NULL THEN 1 ELSE 0 END) as activas,
           SUM(CASE WHEN r.deleted_at IS NOT NULL THEN 1 ELSE 0 END) as eliminadas,
           GROUP_CONCAT(DISTINCT r.estatus) as estatus
    FROM solicitudCaras sc
    LEFT JOIN reservas r ON r.solicitudCaras_id = sc.id
    GROUP BY sc.idquote
    ORDER BY sc.idquote
''')
for r in cursor.fetchall():
    print(f'  propuesta={r["propuesta_id"]} | total={r["total"]} | activas={r["activas"]} | eliminadas={r["eliminadas"]} | estatus={r["estatus"]}')

# 6. Reservas de propuesta 1 con detalle de deleted
print('\n=== RESERVAS PROPUESTA #1 (muestra) ===')
cursor.execute('''
    SELECT r.id, r.estatus, r.deleted_at, r.fecha_reserva
    FROM reservas r
    INNER JOIN solicitudCaras sc ON sc.id = r.solicitudCaras_id
    WHERE sc.idquote = '1'
    ORDER BY r.id
    LIMIT 15
''')
rows = cursor.fetchall()
print(f'  Mostrando {len(rows)} de las reservas:')
for r in rows:
    print(f'    id={r["id"]} | estatus={r["estatus"]} | deleted={r["deleted_at"]} | fecha={r["fecha_reserva"]}')

# 7. Check getAll propuestas query (what frontend sees)
print('\n=== LO QUE VE EL FRONTEND (propuestas sin deleted_at, con solicitud Atendida) ===')
cursor.execute('''
    SELECT pr.id, pr.status, pr.deleted_at, sol.status as sol_status
    FROM propuesta pr
    LEFT JOIN solicitud sol ON sol.id = pr.solicitud_id
    WHERE pr.deleted_at IS NULL
    ORDER BY pr.id
''')
for r in cursor.fetchall():
    print(f'  propuesta={r["id"]} | status={r["status"]} | sol_status={r["sol_status"]}')

# 8. Check the propuestas getAll filter
print('\n=== CHECK: propuestas endpoint filters ===')
cursor.execute('''
    SELECT pr.id, pr.status, sl.status as sol_status
    FROM propuesta pr
    LEFT JOIN solicitud sl ON sl.id = pr.solicitud_id
    WHERE pr.deleted_at IS NULL
      AND pr.status NOT IN ('pendiente', 'Pendiente', 'Sin solicitud activa')
      AND sl.status = 'Atendida'
    ORDER BY pr.id
''')
rows = cursor.fetchall()
print(f'  Propuestas visibles con soloAtendidas: {len(rows)}')
for r in rows:
    print(f'    id={r["id"]} | status={r["status"]} | sol_status={r["sol_status"]}')

cursor.close()
conn.close()
