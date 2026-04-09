"""
Migration Script: CSV -> QEB Database
Cat02-Cat07: CSV unificado (sin usuarios)
Cat08-Cat26: CSV con DIDI + usuarios + asignados

Agrupación por APS Global:
  - Cada APS Global único = una campaña única (su ID)
  - Si la misma campaña tiene DIFERENTE APS en otra catorcena = campañas SEPARADAS
  - Si DIFERENTES nombres comparten MISMO APS = misma campaña, nombre = última catorcena
  - APS 0/#N/A/vacío = auto-increment desde 80000+
  - AUTO_INCREMENT se pone en 80000 ANTES de migrar

Article prefixes:
  RT = Renta (tarifa normal, cortesia=0)
  CT = Cortesia (tarifa=0, cortesia=1, estatus=Bonificado)
  BF/CF = Bonificacion (tarifa normal, cortesia=0, estatus=Bonificado)
  IN = Intercambio (tarifa=0, cortesia=0, estatus=Bonificado)
"""

import csv
import sys
import time
from collections import defaultdict
from datetime import datetime

import mysql.connector

# ============================================================
# CONFIG
# ============================================================

EXTRAS = r"C:\Users\Mario\OneDrive\Documents\QEB\extras"

CSV_FILES = [
    # Cat 02-07: CSV unificado con Formato y Estado corregidos
    {
        "path": EXTRAS + r"\listos-para-cargar\Layout QEB - Cat02-Cat07 - UNIFICADO.csv",
        "catorcena": cat,
        "year": 2026,
        "cara_column": "Cara",
        "segment_filter": f"Catorcena #{cat:02d}",
        "has_users": True,
    }
    for cat in range(2, 8)
] + [
    # Cat 08-26: CSV con DIDI + usuarios + asignados
    {
        "path": EXTRAS + r"\listos-para-cargar\Layout QEB - Cat08-Cat26 - FILLED - CON DIDI - USUARIOS.csv",
        "catorcena": cat,
        "year": 2026,
        "cara_column": "Cara",
        "segment_filter": f"Catorcena #{cat:02d}",
        "has_users": True,
    }
    for cat in range(8, 27)
]

DB_CONFIG = {
    "host": "qeb-mysql-prod-do-user-32408772-0.g.db.ondigitalocean.com",
    "port": 25060,
    "user": "doadmin",
    "password": "AVNS_GUPCVh6o1ZSAOK0EL0J",
    "database": "u658050396_QEB",
    "charset": "utf8mb4",
    "ssl_disabled": False,
}

MAX_CAMPAIGNS = 0  # 0 = todas

CIUDAD_ESTADO_MAP = {
    "Ciudad de México": "Ciudad de México",
    "Guadalajara": "Jalisco", "Zapopan": "Jalisco",
    "San Pedro Tlaquepaque": "Jalisco",
    "Monterrey": "Nuevo León", "San Nicolás de los Garza": "Nuevo León",
    "Guadalupe": "Nuevo León", "Santa Catarina": "Nuevo León",
    "Puebla": "Puebla", "San Andres Cholula": "Puebla", "San Pedro Cholula": "Puebla",
    "León": "Guanajuato", "Mérida": "Yucatán",
    "Tijuana": "Baja California", "Culiacán": "Sinaloa", "Mazatlán": "Sinaloa",
    "Toluca": "Estado de México", "Acapulco de Juárez": "Guerrero",
    "Oaxaca de Juárez": "Oaxaca", "Veracruz": "Veracruz",
    "Boca del Río": "Veracruz", "Puerto Vallarta": "Jalisco",
    "Pachuca de Soto": "Hidalgo", "Aguascalientes": "Aguascalientes",
    "San Luis Potosí": "San Luis Potosí",
}


# ============================================================
# HELPERS
# ============================================================

def parse_price(price_str):
    if not price_str:
        return 0.0
    cleaned = str(price_str).replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_int(val):
    if not val:
        return 0
    cleaned = str(val).strip()
    try:
        return int(float(cleaned))
    except ValueError:
        return 0


def get_cara(row):
    cara = row.get("Cara", "").strip()
    if cara == "*":
        cara = "Contraflujo"
    return cara


def get_article_type(articulo):
    art = articulo.upper()
    if art.startswith("CT"):
        return (1, True, "Vendido bonificado")
    if art.startswith("IN"):
        return (0, True, "Vendido bonificado")
    if art.startswith("BF") or art.startswith("CF"):
        return (0, False, "Vendido bonificado")
    return (0, False, "Vendido")


def is_bonif_type(articulo):
    art = articulo.upper()
    return art.startswith("CT") or art.startswith("BF") or art.startswith("CF") or art.startswith("IN")


def is_garbage_article(articulo):
    art = articulo.strip().upper()
    return art in ("", "0", "#N/A", "N/A", "SIN-ART")


def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def batch_insert_reservas(conn, cursor, reservas_data, max_retries=3):
    if not reservas_data:
        return conn, cursor, 0
    BATCH_SIZE = 50
    total = 0
    for i in range(0, len(reservas_data), BATCH_SIZE):
        batch = reservas_data[i:i + BATCH_SIZE]
        placeholders = ", ".join(
            ["(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"] * len(batch)
        )
        flat_params = []
        for row in batch:
            flat_params.extend(row)
        query = f"""
            INSERT INTO reservas
            (inventario_id, calendario_id, cliente_id, fecha_reserva,
             solicitudCaras_id, archivo, estatus, arte_aprobado,
             comentario_rechazo, estatus_original, fecha_testigo,
             imagen_testigo, instalado, tarea, APS)
            VALUES {placeholders}
        """
        for attempt in range(max_retries):
            try:
                cursor.execute(query, flat_params)
                total += len(batch)
                break
            except (mysql.connector.errors.OperationalError,
                    mysql.connector.errors.InterfaceError,
                    ConnectionResetError) as e:
                print(f"\r      [RECONNECT] Batch insert failed (attempt {attempt+1}): {e}")
                time.sleep(3 * (attempt + 1))
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_connection()
                cursor = conn.cursor(dictionary=True)
        else:
            raise Exception(f"Failed to insert batch after {max_retries} retries")
    return conn, cursor, total


def progress_bar(current, total, prefix="", width=40):
    pct = current / total if total > 0 else 0
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    sys.stdout.write(f"\r  {prefix} [{bar}] {current}/{total} ({pct*100:.1f}%)")
    sys.stdout.flush()


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("MIGRATION: CSV -> QEB_PRUEBAS Database")
    print("Cat 02-07 (unificado) + Cat 08-26 (con DIDI + usuarios)")
    print("=" * 60)

    # --- Connect ---
    print("\n[1] Connecting to database...")
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    print("    Connected!")

    # --- Load catorcenas ---
    print("\n[2] Loading catorcenas...")
    catorcena_cache = {}
    for cat_num in range(2, 27):
        cursor.execute(
            "SELECT id, fecha_inicio, fecha_fin FROM catorcenas WHERE año = %s AND numero_catorcena = %s",
            (2026, cat_num)
        )
        cat = cursor.fetchone()
        if cat:
            catorcena_cache[(2026, cat_num)] = cat
            print(f"    Cat #{cat_num:02d}: {cat['fecha_inicio']} -> {cat['fecha_fin']}")
        else:
            print(f"    WARNING: Cat #{cat_num:02d} not found!")

    # --- Load caches ---
    print("\n[3] Loading caches...")
    cursor.execute("SELECT id, codigo_unico FROM inventarios")
    inv_cache = {}
    inv_by_code = defaultdict(list)
    for r in cursor.fetchall():
        key = r["codigo_unico"].lower() if r["codigo_unico"] else ""
        inv_cache[key] = r["id"]
        code = key.split("_")[0]
        inv_by_code[code].append((key, r["id"]))
    print(f"    Loaded {len(inv_cache)} inventarios")

    cursor.execute("SELECT id, inventario_id FROM espacio_inventario")
    esp_cache = {r["inventario_id"]: r["id"] for r in cursor.fetchall()}
    print(f"    Loaded {len(esp_cache)} espacio_inventario")

    cursor.execute("""
        SELECT id, CUIC, T0_U_Cliente, T0_U_RazonSocial, T0_U_Asesor, T0_U_Agencia,
               T1_U_UnidadNegocio, T2_U_Marca, T2_U_Producto, T2_U_Categoria,
               card_code, salesperson_code, sap_database
        FROM cliente
    """)
    cliente_cache = {}
    for r in cursor.fetchall():
        if r["CUIC"]:
            cliente_cache[int(r["CUIC"])] = r
    print(f"    Loaded {len(cliente_cache)} clientes")

    # --- Load DIDI inventory to skip reservas for non-DIDI campaigns ---
    print("\n[3.5] Loading DIDI inventory list...")
    didi_inventory = set()
    didi_csv_path = EXTRAS + r"\didi\Layout QEB - DIDI Cat08-Cat26 - CORRECTED.csv"
    try:
        with open(didi_csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                unidad = row.get("Unidad", "").strip()
                cara = row.get("Cara", "").strip()
                cat = row.get("Fin o Segmento", "").strip()
                if unidad and cara and cat:
                    didi_inventory.add(f"{unidad}|{cara}|{cat}")
        print(f"    Loaded {len(didi_inventory)} DIDI unit+cara+catorcena combos")
    except FileNotFoundError:
        print("    WARNING: DIDI CSV not found, no inventory will be skipped")

    # --- Set AUTO_INCREMENT ---
    print("\n[3.6] Setting AUTO_INCREMENT = 80000...")
    for table in ["solicitud", "propuesta", "cotizacion", "campania"]:
        cursor.execute(f"ALTER TABLE {table} AUTO_INCREMENT = 80000")
    conn.commit()
    print("    Done!")

    # --- Pre-scan CSVs for APS mapping ---
    print("\n[4] Pre-scanning CSVs for APS Global...")
    aps_latest_name = {}
    aps_all_names = defaultdict(set)

    for csv_file in CSV_FILES:
        cat_num = csv_file["catorcena"]
        seg_filter = csv_file.get("segment_filter")
        with open(csv_file["path"], "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            seen_aps = set()
            for row in reader:
                if seg_filter:
                    seg = row.get("Fin o Segmento", "").strip()
                    if seg != seg_filter:
                        continue
                name = row.get("Campaña", row.get("Campa\u00f1a", "")).strip()
                if not name:
                    continue
                aps_raw = row.get("APS Global", "").strip()
                aps_val = parse_int(aps_raw)
                if aps_val > 0 and aps_raw not in ("#N/A", "N/A"):
                    aps_all_names[aps_val].add(name)
                    if aps_val not in seen_aps:
                        seen_aps.add(aps_val)
                        prev = aps_latest_name.get(aps_val)
                        if not prev or cat_num > prev[0]:
                            aps_latest_name[aps_val] = (cat_num, name)

    multi_name = sum(1 for names in aps_all_names.values() if len(names) > 1)
    print(f"    Total unique valid APS: {len(aps_latest_name)}, multi-name: {multi_name}")

    # --- Track existing campaigns ---
    existing_campaigns = {}

    stats = {
        "campanias_created": 0, "campanias_extended": 0,
        "solicitudes": 0, "propuestas": 0, "cotizaciones": 0,
        "solicitud_caras": 0, "calendarios": 0,
        "reservas": 0, "reservas_skipped": 0,
        "cuics_found": 0, "cuics_not_found": [],
        "units_not_found": [], "rentas": 0, "cortesias": 0,
        "bonificaciones": 0, "intercambios": 0,
    }

    total_csv_files = len(CSV_FILES)

    # --- Process each catorcena ---
    for csv_idx, csv_file in enumerate(CSV_FILES):
        cat_num = csv_file["catorcena"]
        cat_year = csv_file["year"]
        has_users = csv_file.get("has_users", False)
        seg_filter = csv_file.get("segment_filter")
        cat_key = (cat_year, cat_num)

        if cat_key not in catorcena_cache:
            continue
        cat_data = catorcena_cache[cat_key]
        cat_inicio = cat_data["fecha_inicio"]
        cat_fin = cat_data["fecha_fin"]

        print(f"\n{'='*60}")
        print(f"[{csv_idx+1}/{total_csv_files}] Cat #{cat_num:02d} {cat_year} ({cat_inicio} -> {cat_fin})")
        if has_users:
            print(f"  Mode: CON USUARIOS")
        print(f"{'='*60}")

        # Read and group rows by APS
        rows_by_aps = defaultdict(list)
        aps_order = []

        with open(csv_file["path"], "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if seg_filter:
                    seg = row.get("Fin o Segmento", "").strip()
                    if seg != seg_filter:
                        continue
                name = row.get("Campaña", row.get("Campa\u00f1a", "")).strip()
                if not name:
                    continue
                aps_raw = row.get("APS Global", "").strip()
                aps_val = parse_int(aps_raw)
                has_valid_aps = aps_val > 0 and aps_raw not in ("#N/A", "N/A")
                group_key = aps_val if has_valid_aps else f"noaps:{name}"
                if group_key not in rows_by_aps:
                    aps_order.append(group_key)
                rows_by_aps[group_key].append(row)

        print(f"  Rows: {sum(len(v) for v in rows_by_aps.values())}, Groups: {len(aps_order)}")

        campaigns_to_process = aps_order[:MAX_CAMPAIGNS] if MAX_CAMPAIGNS else aps_order

        for idx, group_key in enumerate(campaigns_to_process):
            camp_rows = rows_by_aps[group_key]
            first_row = camp_rows[0]
            camp_name = first_row.get("Campaña", first_row.get("Campa\u00f1a", "")).strip()
            has_valid_aps = not isinstance(group_key, str)

            if has_valid_aps:
                aps_val = group_key
                lookup_key = aps_val
                canonical_name = aps_latest_name[aps_val][1]
            else:
                aps_val = 0
                lookup_key = group_key
                canonical_name = camp_name

            is_existing = lookup_key in existing_campaigns
            same_cat_repeat = is_existing and cat_num in existing_campaigns[lookup_key].get("processed_cats", set())

            if same_cat_repeat:
                continue

            progress_bar(idx + 1, len(campaigns_to_process), f"Cat#{cat_num:02d}")

            try:
                anunciante = first_row.get("Anunciante", "").strip()
                vendedor = first_row.get("Vendedor", "").strip()
                cuic_str = first_row.get("CUIC", "").strip()
                cuic = parse_int(cuic_str)
                articulo = first_row.get("Articulo", "").strip() or "SIN-ART"

                # User data (Cat 8-26 only)
                usuario_id = parse_int(first_row.get("usuario_id", "")) if has_users else 0
                nombre_usuario = first_row.get("nombre_usuario", "").strip() if has_users else vendedor
                asignado = first_row.get("asignado", "").strip() if has_users else vendedor
                id_asignado = first_row.get("id_asignado", "").strip() if has_users else ""

                if is_garbage_article(articulo) and is_existing:
                    original_art = existing_campaigns[lookup_key].get("original_articulo", "SIN-ART")
                    if not is_garbage_article(original_art):
                        articulo = original_art

                is_bonif = is_bonif_type(articulo)

                precio_cat = 0.0
                for r in camp_rows:
                    row_art = r.get("Articulo", "").strip()
                    _, row_tarifa_zero, _ = get_article_type(row_art) if row_art else (0, False, "")
                    if not row_tarifa_zero:
                        precio_cat += parse_price(r.get("Precio por cara (Opcional)", ""))

                total_caras_cat = len(camp_rows)
                bonif_count = total_caras_cat if is_bonif else 0

                # Lookup cliente
                cliente_id = 0
                cli = None
                if cuic and cuic in cliente_cache:
                    cli = cliente_cache[cuic]
                    cliente_id = cli["id"]
                    if not is_existing:
                        stats["cuics_found"] += 1
                elif cuic and not is_existing:
                    stats["cuics_not_found"].append(f"{cuic} ({anunciante})")

                now = datetime.now()

                if is_existing:
                    # ========== EXTEND ==========
                    camp_info = existing_campaigns[lookup_key]
                    campania_id = camp_info["campania_id"]
                    cotizacion_id = camp_info["cotizacion_id"]
                    propuesta_id = camp_info["propuesta_id"]
                    cliente_id = camp_info["cliente_id"]

                    cursor.execute("UPDATE campania SET fecha_fin=%s, nombre=%s WHERE id=%s",
                                   (cat_fin, canonical_name[:100], campania_id))
                    cursor.execute("UPDATE cotizacion SET fecha_fin=%s, nombre_campania=%s WHERE id=%s",
                                   (cat_fin, canonical_name, cotizacion_id))
                    cursor.execute("UPDATE solicitud SET descripcion=%s WHERE id=%s",
                                   (canonical_name, camp_info["solicitud_id"]))
                    cursor.execute("UPDATE campania SET total_caras=total_caras+%s, bonificacion=bonificacion+%s WHERE id=%s",
                                   (total_caras_cat, bonif_count, campania_id))

                    existing_campaigns[lookup_key]["processed_cats"].add(cat_num)
                    stats["campanias_extended"] += 1

                else:
                    # ========== CREATE NEW ==========
                    use_aps_id = has_valid_aps

                    # --- Solicitud ---
                    sol_fields = "fecha, descripcion, presupuesto, notas, cliente_id, status, asignado, id_asignado, " \
                                 "razon_social, IMU, cuic, card_code, salesperson_code, sap_database, " \
                                 "unidad_negocio, marca_nombre, producto_nombre, categoria_nombre, " \
                                 "asesor, agencia, nombre_usuario"
                    sol_vals = (
                        now, canonical_name, precio_cat,
                        f"Migración Cat{cat_num:02d} {cat_year}",
                        cliente_id, "Atendida", asignado, id_asignado,
                        cli["T0_U_RazonSocial"] if cli else anunciante,
                        0, str(cuic),
                        cli["card_code"] if cli else None,
                        cli["salesperson_code"] if cli else None,
                        cli["sap_database"] if cli else None,
                        cli["T1_U_UnidadNegocio"] if cli else None,
                        cli["T2_U_Marca"] if cli else anunciante,
                        cli["T2_U_Producto"] if cli else None,
                        cli["T2_U_Categoria"] if cli else None,
                        cli["T0_U_Asesor"] if cli else vendedor,
                        cli["T0_U_Agencia"] if cli else None,
                        nombre_usuario,
                    )
                    if use_aps_id:
                        cursor.execute(f"INSERT INTO solicitud (id, {sol_fields}) VALUES (%s, {', '.join(['%s']*21)})",
                                       (aps_val, *sol_vals))
                    else:
                        cursor.execute(f"INSERT INTO solicitud ({sol_fields}) VALUES ({', '.join(['%s']*21)})", sol_vals)

                    solicitud_id = aps_val if use_aps_id else cursor.lastrowid
                    if has_users and usuario_id:
                        cursor.execute("UPDATE solicitud SET usuario_id=%s WHERE id=%s",
                                       (usuario_id, solicitud_id))
                    stats["solicitudes"] += 1

                    # --- Propuesta ---
                    prop_fields = "cliente_id, status, solicitud_id, asignado, id_asignado, inversion, articulo, comentario_cambio_status, fecha, descripcion, notas"
                    prop_vals = (cliente_id, "Aprobada", solicitud_id, asignado, id_asignado, precio_cat, articulo, "Migrado", now, canonical_name, f"Migración Cat{cat_num:02d} {cat_year}")
                    if use_aps_id:
                        cursor.execute(f"INSERT INTO propuesta (id, {prop_fields}) VALUES (%s, {', '.join(['%s']*11)})",
                                       (aps_val, *prop_vals))
                    else:
                        cursor.execute(f"INSERT INTO propuesta ({prop_fields}) VALUES ({', '.join(['%s']*11)})", prop_vals)
                    propuesta_id = aps_val if use_aps_id else cursor.lastrowid
                    stats["propuestas"] += 1

                    # --- Cotización ---
                    caras_flujo = sum(1 for r in camp_rows if get_cara(r) == "Flujo")
                    caras_contra = sum(1 for r in camp_rows if get_cara(r) == "Contraflujo")
                    cot_fields = "user_id, clientes_id, nombre_campania, numero_caras, fecha_inicio, fecha_fin, " \
                                 "frontal, cruzada, observaciones, bonificacion, descuento, precio, contacto, " \
                                 "status, id_propuesta, articulo"
                    cot_vals = (
                        usuario_id or 0, cliente_id, canonical_name, total_caras_cat,
                        cat_inicio, cat_fin,
                        caras_flujo if not is_bonif else 0,
                        caras_contra if not is_bonif else 0,
                        "Migración", bonif_count, 0, precio_cat, vendedor,
                        "Aprobada", propuesta_id, articulo
                    )
                    if use_aps_id:
                        cursor.execute(f"INSERT INTO cotizacion (id, {cot_fields}) VALUES (%s, {', '.join(['%s']*16)})",
                                       (aps_val, *cot_vals))
                    else:
                        cursor.execute(f"INSERT INTO cotizacion ({cot_fields}) VALUES ({', '.join(['%s']*16)})", cot_vals)
                    cotizacion_id = aps_val if use_aps_id else cursor.lastrowid
                    stats["cotizaciones"] += 1

                    # --- Campaña ---
                    camp_fields = "cliente_id, nombre, fecha_inicio, fecha_fin, total_caras, bonificacion, " \
                                  "status, cotizacion_id, articulo, fecha_aprobacion"
                    camp_vals = (
                        cliente_id, canonical_name[:100], cat_inicio, cat_fin,
                        str(total_caras_cat), bonif_count,
                        "Aprobada", cotizacion_id, articulo, now
                    )
                    if use_aps_id:
                        cursor.execute(f"INSERT INTO campania (id, {camp_fields}) VALUES (%s, {', '.join(['%s']*10)})",
                                       (aps_val, *camp_vals))
                    else:
                        cursor.execute(f"INSERT INTO campania ({camp_fields}) VALUES ({', '.join(['%s']*10)})", camp_vals)
                    campania_id = aps_val if use_aps_id else cursor.lastrowid
                    stats["campanias_created"] += 1

                # ---- SolicitudCaras (both NEW and EXTEND) ----
                if is_existing:
                    propuesta_id = existing_campaigns[lookup_key]["propuesta_id"]

                rows_by_art = defaultdict(list)
                original_art = existing_campaigns.get(lookup_key, {}).get("original_articulo", articulo)
                for r in camp_rows:
                    key = r.get("Articulo", "").strip()
                    if is_garbage_article(key) and not is_garbage_article(original_art):
                        key = original_art
                    rows_by_art[key].append(r)

                solicitud_caras_map = {}
                for row_art, group_rows in rows_by_art.items():
                    # Use corrected Ciudad/Estado from CSV
                    ciudades = sorted(set(r.get("Ciudad", "").strip() for r in group_rows if r.get("Ciudad", "").strip()))
                    # Use Estado column if available, otherwise fallback to map
                    estados_from_csv = set(r.get("Estado", "").strip() for r in group_rows if r.get("Estado", "").strip())
                    if estados_from_csv:
                        estados = sorted(estados_from_csv)
                    else:
                        estados = sorted(set(CIUDAD_ESTADO_MAP.get(c, c) for c in ciudades))
                    ciudad_str = ", ".join(ciudades)
                    estado_str = ", ".join(estados)

                    # Use Formato from CSV if available
                    formatos = set(r.get("Formato", "").strip() for r in group_rows if r.get("Formato", "").strip())
                    formato = sorted(formatos)[0] if formatos else "PARABUS"

                    # Use tipo from CSV or detect from article
                    tipo = "Tradicional"
                    art_upper = row_art.upper()
                    if "DIG" in art_upper or "PRG" in art_upper:
                        tipo = "Digital"

                    grp_caras = len(group_rows)
                    grp_flujo = sum(1 for r in group_rows if get_cara(r) == "Flujo")
                    grp_contra = sum(1 for r in group_rows if get_cara(r) == "Contraflujo")

                    row_cortesia, row_tarifa_zero, _ = get_article_type(row_art)
                    row_is_bonif = is_bonif_type(row_art)

                    if row_tarifa_zero:
                        grp_precio = 0.0
                        grp_tarifa = 0.0
                    else:
                        grp_precio = sum(parse_price(r.get("Precio por cara (Opcional)", "")) for r in group_rows)
                        grp_tarifa = round(grp_precio / grp_caras, 2) if grp_caras > 0 else 0

                    renta_caras = 0 if row_is_bonif else grp_caras
                    bonif_caras = grp_caras if row_is_bonif else 0

                    # Get NSE from CSV rows (use most common value in group)
                    nse_values = [r.get("NSE", "").strip() for r in group_rows if r.get("NSE", "").strip()]
                    nse_str = sorted(set(nse_values))[0] if nse_values else "Todos"

                    cursor.execute("""
                        INSERT INTO solicitudCaras
                        (idquote, ciudad, estados, tipo, flujo, caras, nivel_socioeconomico,
                         formato, costo, tarifa_publica, inicio_periodo, fin_periodo,
                         caras_flujo, caras_contraflujo, articulo, bonificacion, cortesia)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        str(propuesta_id), ciudad_str, estado_str, tipo, "Ambos",
                        renta_caras, nse_str, formato, grp_precio, grp_tarifa,
                        cat_inicio, cat_fin,
                        grp_flujo if not row_is_bonif else 0,
                        grp_contra if not row_is_bonif else 0,
                        row_art, bonif_caras, row_cortesia
                    ))
                    sc_id = cursor.lastrowid
                    solicitud_caras_map[row_art] = sc_id
                    stats["solicitud_caras"] += 1

                if not is_existing:
                    existing_campaigns[lookup_key] = {
                        "campania_id": campania_id,
                        "cotizacion_id": cotizacion_id,
                        "propuesta_id": propuesta_id,
                        "solicitud_id": solicitud_id,
                        "solicitud_caras_map": solicitud_caras_map,
                        "cliente_id": cliente_id,
                        "processed_cats": {cat_num},
                        "original_articulo": articulo,
                    }
                else:
                    existing_campaigns[lookup_key]["processed_cats"].add(cat_num)
                    existing_campaigns[lookup_key]["solicitud_caras_map"].update(solicitud_caras_map)

                conn.commit()

                # ---- Calendario ----
                cursor.execute("INSERT INTO calendario (fecha_inicio, fecha_fin) VALUES (%s, %s)",
                               (cat_inicio, cat_fin))
                calendario_id = cursor.lastrowid
                stats["calendarios"] += 1

                # ---- Reservas ----
                is_didi_campaign = "didi" in canonical_name.lower()
                reservas_data = []
                skipped = 0
                didi_skipped = 0
                for r in camp_rows:
                    unidad = r.get("Unidad", "").strip()
                    cara = get_cara(r)
                    ciudad = r.get("Ciudad", "").strip()
                    row_articulo = r.get("Articulo", "").strip()
                    seg = r.get("Fin o Segmento", "").strip()

                    # Skip DIDI inventory for non-DIDI campaigns (leave caras expected but no reserva)
                    if not is_didi_campaign and f"{unidad}|{cara}|{seg}" in didi_inventory:
                        didi_skipped += 1
                        continue

                    codigo = f"{unidad}_{cara}_{ciudad}".lower()
                    inv_id = inv_cache.get(codigo)

                    if not inv_id:
                        prefix = f"{unidad}_{cara}_".lower()
                        for key, val in inv_cache.items():
                            if key.startswith(prefix):
                                inv_id = val
                                break

                    if not inv_id:
                        # Try by code only
                        code_matches = inv_by_code.get(unidad.lower(), [])
                        if code_matches:
                            inv_id = code_matches[0][1]

                    if not inv_id:
                        stats["units_not_found"].append(f"{unidad}_{cara}_{ciudad}")
                        skipped += 1
                        continue

                    esp_id = esp_cache.get(inv_id)
                    if not esp_id:
                        skipped += 1
                        continue

                    _, _, estatus = get_article_type(row_articulo)
                    sc_id = solicitud_caras_map.get(row_articulo, list(solicitud_caras_map.values())[0] if solicitud_caras_map else 0)
                    row_aps = parse_int(r.get("Código de contrato (Opcional)", "").strip())

                    reservas_data.append((
                        esp_id, calendario_id, cliente_id, now.date(),
                        sc_id, None, estatus, "Pendiente",
                        "", estatus, now, "", 0, "", row_aps
                    ))

                conn, cursor, reservas_count = batch_insert_reservas(conn, cursor, reservas_data)
                stats["reservas"] += reservas_count
                stats["reservas_skipped"] += skipped
                if didi_skipped > 0:
                    print(f"\r      -> {reservas_count} reservas, {skipped} skipped, {didi_skipped} DIDI-reserved (no reserva)")
                conn.commit()

            except Exception as e:
                print(f"\n      ERROR: {e}")
                import traceback
                traceback.print_exc()
                try:
                    conn.rollback()
                except Exception:
                    conn = get_connection()
                    cursor = conn.cursor(dictionary=True)

        print()  # newline after progress bar

    # --- Final AUTO_INCREMENT ---
    print("\n[AUTO_INCREMENT] Final adjustment...")
    for table in ["solicitud", "propuesta", "cotizacion", "campania"]:
        try:
            cursor.execute(f"SELECT MAX(id) as max_id FROM {table}")
            result = cursor.fetchone()
            max_id = result["max_id"] or 0
            new_auto = max(80000, max_id + 1)
            cursor.execute(f"ALTER TABLE {table} AUTO_INCREMENT = {new_auto}")
            print(f"    {table}: AUTO_INCREMENT = {new_auto}")
        except Exception as e:
            print(f"    ERROR: {e}")
    conn.commit()

    # --- Report ---
    print("\n" + "=" * 60)
    print("MIGRATION REPORT")
    print("=" * 60)
    print(f"  Campanias created:  {stats['campanias_created']}")
    print(f"  Campanias extended: {stats['campanias_extended']}")
    print(f"  CUICs found:       {stats['cuics_found']}")
    print(f"  Solicitudes:       {stats['solicitudes']}")
    print(f"  Propuestas:        {stats['propuestas']}")
    print(f"  Cotizaciones:      {stats['cotizaciones']}")
    print(f"  SolicitudCaras:    {stats['solicitud_caras']}")
    print(f"  Calendarios:       {stats['calendarios']}")
    print(f"  Reservas:          {stats['reservas']}")
    print(f"  Reservas skipped:  {stats['reservas_skipped']}")
    print(f"  Units not found:   {len(set(stats['units_not_found']))}")

    if stats["cuics_not_found"]:
        unique_cuics = sorted(set(stats["cuics_not_found"]))
        print(f"\n  CUICs NOT found ({len(unique_cuics)}):")
        for c in unique_cuics[:20]:
            print(f"    - {c}")

    if stats["units_not_found"]:
        unique_missing = sorted(set(stats["units_not_found"]))
        print(f"\n  Units not found ({len(unique_missing)} unique):")
        for u in unique_missing[:30]:
            print(f"    - {u}")
        if len(unique_missing) > 30:
            print(f"    ... and {len(unique_missing) - 30} more")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
