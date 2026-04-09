"""
Copy static/reference data from QEB to QEB_PRUEBAS.
Tables that are NOT transactional (not in reset_database.sql).
"""
import mysql.connector

# Tables to copy (non-transactional / reference data)
TABLES_TO_COPY = [
    'usuario',
    'cliente',
    'catorcenas',
    'espacio_inventario',
    'inventarios',
    'proveedores',
    'equipo',
    'usuario_equipo',
    'criterios_autorizacion',
    'conteoTradicionalYDigital',
    'vista_conteo_por_ciudades',
    'vista_conteo_por_muebles',
]

src = mysql.connector.connect(
    host='srv1978.hstgr.io', port=3306,
    user='u658050396_QEB', password='QEBdevelop.1',
    database='u658050396_QEB', charset='utf8mb4'
)

dst = mysql.connector.connect(
    host='82.197.82.225', port=3306,
    user='u658050396_QEB_PRUEBAS', password='/uQ3FCrLG5:6',
    database='u658050396_QEB_PRUEBAS', charset='utf8mb4'
)

src_cursor = src.cursor()
dst_cursor = dst.cursor()

dst_cursor.execute('SET FOREIGN_KEY_CHECKS = 0')

for table in TABLES_TO_COPY:
    try:
        # Check if table exists and has data in source
        src_cursor.execute(f'SELECT COUNT(*) FROM `{table}`')
        count = src_cursor.fetchone()[0]

        if count == 0:
            print(f'  {table}: empty, skipping')
            continue

        # Clear destination table
        dst_cursor.execute(f'TRUNCATE TABLE `{table}`')

        # Get column names
        src_cursor.execute(f'SHOW COLUMNS FROM `{table}`')
        columns = [row[0] for row in src_cursor.fetchall()]
        cols_str = ', '.join([f'`{c}`' for c in columns])
        placeholders = ', '.join(['%s'] * len(columns))

        # Read all data
        src_cursor.execute(f'SELECT {cols_str} FROM `{table}`')
        rows = src_cursor.fetchall()

        # Insert in batches
        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            dst_cursor.executemany(
                f'INSERT INTO `{table}` ({cols_str}) VALUES ({placeholders})',
                batch
            )

        dst.commit()
        print(f'  {table}: {len(rows)} rows copied')

    except Exception as e:
        print(f'  {table}: ERROR - {e}')
        dst.rollback()

dst_cursor.execute('SET FOREIGN_KEY_CHECKS = 1')
dst.commit()

src_cursor.close()
dst_cursor.close()
src.close()
dst.close()

print('\nStatic data copy completed!')
