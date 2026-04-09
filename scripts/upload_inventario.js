/**
 * Script para subir/actualizar inventario desde CSV
 *
 * Uso:
 *   node upload_inventario.js pruebas    (para QEB_PRUEBAS en Hostinger)
 *   node upload_inventario.js produccion (para QEB en DigitalOcean)
 */

const fs = require('fs');
const { PrismaClient } = require('@prisma/client');

const ENV = process.argv[2] || 'pruebas';
const CSV_PATH = 'C:/Users/Mario/Downloads/Inventario Grupo IMU_Nuevo - Sheet 1.csv';

const DB_URLS = {
  pruebas: 'mysql://u658050396_QEB_PRUEBAS:%2FuQ3FCrLG5%3A6@srv1978.hstgr.io:3306/u658050396_QEB_PRUEBAS?connection_limit=15&pool_timeout=30&connect_timeout=30',
  produccion: 'mysql://doadmin:AVNS_GUPCVh6o1ZSAOK0EL0J@qeb-mysql-prod-do-user-32408772-0.g.db.ondigitalocean.com:25060/u658050396_QEB?sslcert=C%3A%5CUsers%5CMario%5CDownloads%5Cca-certificate.crt&sslaccept=strict',
};

if (!DB_URLS[ENV]) {
  console.error('Uso: node upload_inventario.js [pruebas|produccion]');
  process.exit(1);
}

process.env.DATABASE_URL = DB_URLS[ENV];
const prisma = new PrismaClient();

function parseCsvLine(line) {
  const fields = [];
  let current = '', inQ = false;
  for (const ch of line) {
    if (ch === '"') inQ = !inQ;
    else if (ch === ',' && !inQ) { fields.push(current.trim()); current = ''; }
    else current += ch;
  }
  fields.push(current.trim());
  return fields;
}

function progressBar(current, total, label = '') {
  const pct = Math.round((current / total) * 100);
  const filled = Math.round(pct / 2);
  const bar = '█'.repeat(filled) + '░'.repeat(50 - filled);
  process.stdout.write(`\r${bar} ${pct}% (${current}/${total}) ${label}`);
}

async function main() {
  console.log(`\n🎯 Subiendo inventario a: ${ENV.toUpperCase()}\n`);

  // 1. Read CSV
  const text = fs.readFileSync(CSV_PATH, 'utf-8');
  const lines = text.split(/\r?\n/).filter(l => l.trim());

  const rows = [];
  for (let i = 2; i < lines.length; i++) {
    const f = parseCsvLine(lines[i]);
    if (!f[1]) continue;
    rows.push({
      codigo: f[0] || null,
      codigo_unico: f[1],
      ubicacion: f[2] || null,
      tipo_de_cara: f[3] || null,
      cara: f[4] || null,
      mueble: f[5] || null,
      latitud: parseFloat(f[6]) || 0,
      longitud: parseFloat(f[7]) || 0,
      plaza: f[8] || null,
      estado: f[9] || null,
      municipio: f[10] || null,
      cp: f[11] ? parseInt(f[11]) || null : null,
      tradicional_digital: f[12] || null,
      sentido: f[13] || null,
      isla: f[14] || null,
      mueble_isla: f[15] || null,
      mundialista: f[16] || null,
      entre_calle_1: f[17] || null,
      entre_calle_2: f[18] || null,
      orientacion: f[19] || null,
      tipo_de_mueble: f[20] || null,
      ancho: parseFloat(f[21]) || null,
      alto: parseFloat(f[22]) || null,
      tarifa_piso: f[24] ? parseFloat(f[24]) || null : null,
      tarifa_publica: f[25] ? parseFloat(f[25]) || null : null,
      nivel_socioeconomico: f[26] || null,
      total_espacios: f[27] ? parseInt(f[27]) || null : null,
      tiempo: f[28] ? parseInt(f[28]) || null : null,
      estatus: 'Disponible',
    });
  }
  console.log(`📄 CSV: ${rows.length} filas\n`);

  // 2. Create mundialista column if needed
  const cols = await prisma.$queryRawUnsafe("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='inventarios' AND COLUMN_NAME='mundialista'");
  if (cols.length === 0) {
    await prisma.$executeRawUnsafe("ALTER TABLE inventarios ADD COLUMN mundialista VARCHAR(10) DEFAULT NULL");
    console.log('✅ Columna mundialista creada\n');
  }

  // 3. Check existing
  const codigos = rows.map(r => r.codigo_unico);
  const existing = await prisma.inventarios.findMany({ where: { codigo_unico: { in: codigos } }, select: { id: true, codigo_unico: true } });
  const existingMap = new Map(existing.map(e => [e.codigo_unico, e.id]));

  const toInsert = [];
  const toUpdate = [];
  for (const row of rows) {
    const existId = existingMap.get(row.codigo_unico);
    if (existId) {
      toUpdate.push({ ...row, _id: existId });
    } else {
      toInsert.push(row);
    }
  }
  console.log(`📊 Existentes: ${toUpdate.length} | Nuevos: ${toInsert.length}\n`);

  // 4. Update existing (no toucha estatus)
  if (toUpdate.length > 0) {
    console.log('🔄 Actualizando existentes...');
    for (let i = 0; i < toUpdate.length; i++) {
      const row = toUpdate[i];
      await prisma.$executeRawUnsafe(
        'UPDATE inventarios SET mundialista=?, ubicacion=?, tipo_de_cara=?, cara=?, mueble=?, latitud=?, longitud=?, plaza=?, estado=?, municipio=?, cp=?, tradicional_digital=?, sentido=?, isla=?, mueble_isla=?, entre_calle_1=?, entre_calle_2=?, orientacion=?, tipo_de_mueble=?, nivel_socioeconomico=?, total_espacios=?, codigo=? WHERE id=?',
        row.mundialista, row.ubicacion, row.tipo_de_cara, row.cara, row.mueble, row.latitud, row.longitud, row.plaza, row.estado, row.municipio, row.cp, row.tradicional_digital, row.sentido, row.isla, row.mueble_isla, row.entre_calle_1, row.entre_calle_2, row.orientacion, row.tipo_de_mueble, row.nivel_socioeconomico, row.total_espacios, row.codigo, row._id
      );
      progressBar(i + 1, toUpdate.length, 'updates');
    }
    console.log('\n');
  }

  // 5. Insert new (without mundialista - Prisma doesn't know it)
  if (toInsert.length > 0) {
    console.log('➕ Insertando nuevos...');
    const BATCH = 100;
    for (let i = 0; i < toInsert.length; i += BATCH) {
      const batch = toInsert.slice(i, i + BATCH).map(({ mundialista, ...rest }) => rest);
      await prisma.inventarios.createMany({ data: batch, skipDuplicates: true });
      progressBar(Math.min(i + BATCH, toInsert.length), toInsert.length, 'inserts');
    }
    console.log('\n');

    // Set mundialista via raw UPDATE for new rows
    for (const row of toInsert) {
      if (row.mundialista && row.codigo_unico) {
        await prisma.$executeRawUnsafe('UPDATE inventarios SET mundialista = ? WHERE codigo_unico = ?', row.mundialista, row.codigo_unico);
      }
    }

    // Create espacio_inventario
    const newCodigos = toInsert.map(r => r.codigo_unico);
    const inserted = await prisma.inventarios.findMany({ where: { codigo_unico: { in: newCodigos } }, select: { id: true, tradicional_digital: true, total_espacios: true } });
    const espacios = inserted.flatMap(inv => {
      const isDigital = inv.tradicional_digital === 'Digital' && inv.total_espacios && inv.total_espacios > 0;
      const n = isDigital ? inv.total_espacios : 1;
      return Array.from({ length: n }, (_, j) => ({ inventario_id: inv.id, numero_espacio: j + 1 }));
    });
    if (espacios.length > 0) {
      for (let i = 0; i < espacios.length; i += 500) {
        await prisma.espacio_inventario.createMany({ data: espacios.slice(i, i + 500) });
      }
      console.log(`✅ ${espacios.length} espacio_inventario creados\n`);
    }
  }

  console.log('🎉 ¡Listo!\n');
  await prisma.$disconnect();
}

main().catch(e => { console.error('❌ Error:', e.message); process.exit(1); });
