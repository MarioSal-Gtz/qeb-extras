import mysql.connector
conn = mysql.connector.connect(host='82.197.82.225', port=3306, user='u658050396_QEB_PRUEBAS', password='/uQ3FCrLG5:6', database='u658050396_QEB_PRUEBAS', charset='utf8mb4')
cursor = conn.cursor(dictionary=True)
cursor.execute("SELECT id, LEFT(archivo, 100) as archivo_preview, LENGTH(archivo) as archivo_len FROM reservas WHERE archivo IS NOT NULL AND archivo <> '' LIMIT 10")
for r in cursor.fetchall():
    print(f"ID={r['id']} | len={r['archivo_len']} | preview={r['archivo_preview']}")

# Also check imagenes_digitales
print("\n--- imagenes_digitales ---")
cursor.execute("SELECT id, id_reserva, LEFT(archivo, 150) as archivo_preview FROM imagenes_digitales LIMIT 10")
for r in cursor.fetchall():
    print(f"ID={r['id']} | rsv={r['id_reserva']} | archivo={r['archivo_preview']}")

print("\n--- count imagenes_digitales ---")
cursor.execute("SELECT COUNT(*) as cnt FROM imagenes_digitales")
print(cursor.fetchone())

conn.close()
