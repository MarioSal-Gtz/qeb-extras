const mysql = require('mysql2/promise');
const csv = require('csv-parse/sync');
const fs = require('fs');

async function main() {
  const conn = await mysql.createConnection({
    host: '82.197.82.225', port: 3306,
    user: 'u658050396_QEB_PRUEBAS', password: '/uQ3FCrLG5:6',
    database: 'u658050396_QEB_PRUEBAS'
  });

  // Get all inventarios
  const [invRows] = await conn.query('SELECT id, codigo_unico FROM inventarios');
  const invCache = {};
  invRows.forEach(r => { invCache[r.codigo_unico] = r.id; });

  // Get all espacio_inventario
  const [espRows] = await conn.query('SELECT id, inventario_id FROM espacio_inventario');
  const espCache = {};
  espRows.forEach(r => { espCache[r.inventario_id] = r.id; });

  // Read CSV
  const csvPath = String.raw`C:\Users\Mario\Downloads\validaciónCat02 - 2026 Final INVIAN Carga QEB (1) - Campañas, Artes y Caras Unid..csv`;
  const content = fs.readFileSync(csvPath, 'utf-8');
  const records = csv.parse(content, { columns: true, bom: true, skip_empty_lines: true });

  const campaignsToCheck = ['EL MOCHAOREJAS', 'IMU CORTESIA GENERICA'];

  for (const campName of campaignsToCheck) {
    console.log(`\n=== ${campName} ===`);
    const rows = records.filter(r => {
      const name = (r['Campaña'] || r['Campa\u00f1a'] || '').trim();
      return name.includes(campName);
    });

    console.log(`Filas en CSV: ${rows.length}`);
    const missing = [];

    for (const row of rows) {
      const unidad = (row['Unidad'] || '').trim();
      const cara = (row[' '] || '').trim();  // column with space name
      const ciudad = (row['Ciudad'] || '').trim();
      let codigo = `${unidad}_${cara}_${ciudad}`.replace(/\s+_/g, '_').replace(/_\s+/g, '_');

      let invId = invCache[codigo];
      if (!invId) {
        // Try partial match
        for (const [key, val] of Object.entries(invCache)) {
          if (key.startsWith(`${unidad}_${cara}_`)) {
            invId = val;
            codigo = key + ' (matched partial)';
            break;
          }
        }
      }

      let reason = null;
      if (!invId) {
        reason = 'No existe en inventarios';
      } else {
        const espId = espCache[invId];
        if (!espId) {
          reason = 'Sin espacio_inventario';
        }
      }

      if (reason) {
        missing.push({ unidad, cara, ciudad, codigo, reason });
      }
    }

    console.log(`OK: ${rows.length - missing.length}, Faltantes: ${missing.length}`);
    for (const m of missing) {
      console.log(`  ${m.unidad} | ${m.cara} | ${m.ciudad}`);
      console.log(`    Buscado: ${m.codigo}`);
      console.log(`    Razon: ${m.reason}`);
    }
  }

  await conn.end();
}

main().catch(e => console.error(e));
