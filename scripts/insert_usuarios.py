"""
Insert usuarios from CSV into QEB database.
Password: Admin123 (bcrypt hashed)
"""

import csv
import mysql.connector

CSV_PATH = r"C:\Users\Mario\Downloads\Matriz de usuarios QEB 280126(Directorio).csv"

DB_CONFIG = {
    "host": "srv1978.hstgr.io",
    "port": 3306,
    "user": "u658050396_QEB",
    "password": "QEBdevelop.1",
    "database": "u658050396_QEB",
    "charset": "utf8mb4",
}

# bcrypt hash of "Admin123" with salt rounds 10 (compatible with bcryptjs)
HASHED_PASSWORD = "$2b$10$bkEnlvlEAJT3a1bNH/UmeeJ4Ffr4q0BnxJMPHkbfhdAnCCaxVHjxu"

def main():
    # Read CSV
    with open(CSV_PATH, "r", encoding="latin-1") as f:
        reader = csv.reader(f, delimiter=";")
        rows = list(reader)

    # Skip first 4 rows (empty + header)
    data_rows = rows[4:]

    usuarios = []
    current_area = ""

    for row in data_rows:
        num = row[0].strip() if len(row) > 0 else ""
        area = row[1].strip().replace("\n", " ") if len(row) > 1 else ""
        puesto = row[2].strip() if len(row) > 2 else ""
        nombre = row[3].strip() if len(row) > 3 else ""
        correo = row[4].strip() if len(row) > 4 else ""

        # Skip rows without # or nombre
        if not num or not nombre:
            continue

        # Update current area if provided
        if area:
            current_area = area

        # Default puesto if empty
        if not puesto:
            puesto = current_area or "General"

        # Truncate area and puesto to 50 chars (DB limit)
        current_area_trunc = current_area[:50] if current_area else "General"
        puesto_trunc = puesto[:50]

        usuarios.append({
            "nombre": nombre,
            "correo_electronico": correo,
            "user_password": HASHED_PASSWORD,
            "area": current_area_trunc,
            "puesto": puesto_trunc,
            "user_role": "Normal",
        })

    print(f"Found {len(usuarios)} usuarios to insert:\n")
    for i, u in enumerate(usuarios, 1):
        print(f"  {i:3d}. {u['nombre']:<40s} | {u['correo_electronico']:<30s} | {u['area']:<30s} | {u['puesto']}")

    print(f"\nTotal: {len(usuarios)}")
    confirm = input("\nInsert into database? (y/n): ").strip().lower()
    if confirm != "y":
        print("Cancelled.")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    inserted = 0
    skipped = 0

    for u in usuarios:
        # Check if correo already exists
        cursor.execute(
            "SELECT id FROM usuario WHERE correo_electronico = %s",
            (u["correo_electronico"],),
        )
        if cursor.fetchone():
            print(f"  SKIP (ya existe): {u['correo_electronico']}")
            skipped += 1
            continue

        cursor.execute(
            """INSERT INTO usuario (nombre, correo_electronico, user_password, area, puesto, user_role, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, NOW())""",
            (
                u["nombre"],
                u["correo_electronico"],
                u["user_password"],
                u["area"],
                u["puesto"],
                u["user_role"],
            ),
        )
        inserted += 1
        print(f"  OK: {u['nombre']} ({u['correo_electronico']})")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\nDone! Inserted: {inserted}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
