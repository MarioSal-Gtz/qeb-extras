import csv, mysql.connector

DB_CONFIG = {'host': '82.197.82.225', 'port': 3306, 'user': 'u658050396_QEB_PRUEBAS', 'password': '/uQ3FCrLG5:6', 'database': 'u658050396_QEB_PRUEBAS', 'charset': 'utf8mb4'}
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)

cursor.execute('SELECT id, codigo_unico FROM inventarios')
inv_cache = {r['codigo_unico']: r['id'] for r in cursor.fetchall()}

cursor.execute('SELECT id, inventario_id FROM espacio_inventario')
esp_cache = {r['inventario_id']: r['id'] for r in cursor.fetchall()}

CSV_PATH = r"C:\Users\Mario\Downloads\validaciónCat02 - 2026 Final INVIAN Carga QEB (1) - Campañas, Artes y Caras Unid..csv"

missing = []
total = 0
with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        name = row.get('Campaña', row.get('Campa\u00f1a', '')).strip()
        if name != 'FEMSA, IMUNDIALISTA 2026':
            continue
        total += 1
        unidad = row.get('Unidad', '').strip()
        cara = row.get(' ', '').strip()
        ciudad = row.get('Ciudad', '').strip()
        codigo = f"{unidad}_{cara}_{ciudad}"

        inv_id = inv_cache.get(codigo)
        if not inv_id:
            for key, val in inv_cache.items():
                if key.startswith(f"{unidad}_{cara}_"):
                    inv_id = val
                    break

        reason = None
        if not inv_id:
            reason = 'No existe en inventarios'
        else:
            esp_id = esp_cache.get(inv_id)
            if not esp_id:
                reason = 'Sin espacio_inventario'

        if reason:
            missing.append((unidad, cara, ciudad, codigo, reason))

print(f"FEMSA: {total} filas, {total - len(missing)} OK, {len(missing)} faltantes\n")
for u, c, ci, cod, r in missing:
    print(f"  {u}  |  {c}  |  {ci}")
    print(f"    Buscado: {cod}")
    print(f"    Razon: {r}\n")

conn.close()
