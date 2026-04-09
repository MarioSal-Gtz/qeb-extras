/**
 * Compare Cat 08 Excel data vs QEB Database
 * Only compares rows with status "CARGADA EN QEB"
 * Outputs a CSV with mismatches
 */
const fs = require('fs');
const { PrismaClient } = require('@prisma/client');

const CSV_PATH = 'C:/Users/Mario/Downloads/Orden de Montaje CAT08 - 2026 APS GLOBALES - Previa (1).xlsx - Hoja1.csv';
const OUTPUT_PATH = 'C:/Users/Mario/Downloads/comparacion_cat08_resultado.csv';

function parseCSV(text) {
  const lines = text.split('\n').filter(l => l.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const fields = [];
    let current = '', inQ = false;
    for (const ch of lines[i]) {
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { fields.push(current.trim()); current = ''; }
      else current += ch;
    }
    fields.push(current.trim());
    rows.push({
      ciudad_excel: fields[0] || '',
      tipo: fields[1] || '',
      asesor: fields[3] || '',
      aps_especifico: fields[4] || '',
      aps_global: fields[5] || '',
      cuic: fields[6] || '',
      estatus_aps: fields[7] || '',
      fecha_inicio: fields[8] || '',
      fecha_fin: fields[9] || '',
      cliente: fields[10] || '',
      marca: fields[11] || '',
      campana: fields[12] || '',
      articulo: fields[13] || '',
      articulo_desc: fields[14] || '',
      caras: parseInt((fields[15] || '0').replace(/,/g, '')) || 0,
      tarifa: parseFloat((fields[16] || '0').replace(/[$,]/g, '')) || 0,
      monto_total: parseFloat((fields[17] || '0').replace(/[$,]/g, '')) || 0,
    });
  }
  return rows;
}

async function main() {
  const p = new PrismaClient();
  const text = fs.readFileSync(CSV_PATH, 'utf-8');
  const excelRows = parseCSV(text);

  // Only compare rows that are "CARGADA EN QEB"
  const cargadas = excelRows.filter(r => r.estatus_aps === 'CARGADA EN QEB');
  console.log(`Total rows: ${excelRows.length}, Cargadas en QEB: ${cargadas.length}`);

  // Group Excel by APS Global + Articulo (to compare with solicitudCaras)
  const excelGroups = {};
  for (const row of cargadas) {
    const key = `${row.aps_global}|${row.articulo}`;
    if (!excelGroups[key]) {
      excelGroups[key] = { ...row, caras_total: 0, monto_total_sum: 0, rows: 0 };
    }
    excelGroups[key].caras_total += row.caras;
    excelGroups[key].monto_total_sum += row.monto_total;
    excelGroups[key].rows++;
  }

  console.log(`Unique APS+Articulo groups: ${Object.keys(excelGroups).length}`);

  // Get all solicitudCaras for cat 8 (14 abr - 27 abr 2026)
  const dbCaras = await p.$queryRawUnsafe(`
    SELECT sc.id, sc.idquote, sc.articulo, sc.caras, sc.bonificacion, sc.tarifa_publica, sc.costo, sc.ciudad
    FROM solicitudCaras sc
    WHERE sc.inicio_periodo >= '2026-04-14' AND sc.inicio_periodo <= '2026-04-27'
  `);

  console.log(`DB caras for cat 8: ${dbCaras.length}`);

  // Index DB by idquote (APS) + articulo
  const dbGroups = {};
  for (const cara of dbCaras) {
    const key = `${cara.idquote}|${cara.articulo}`;
    dbGroups[key] = cara;
  }

  // Compare
  const results = [];
  for (const [key, excel] of Object.entries(excelGroups)) {
    const db = dbGroups[key];
    const aps = excel.aps_global;
    const articulo = excel.articulo;

    // Determine expected caras/bonif based on articulo prefix
    const prefix = articulo.substring(0, 2);
    const isRenta = prefix === 'RT';
    const isBonif = prefix === 'BF' || prefix === 'CF';
    const isIntercambio = prefix === 'IN';
    const isCortesia = prefix === 'CT';

    if (!db) {
      results.push({
        aps_global: aps,
        articulo,
        campana: excel.campana,
        cliente: excel.cliente,
        problema: 'NO EXISTE EN BD',
        excel_caras: excel.caras_total,
        excel_tarifa: excel.tarifa,
        excel_monto: excel.monto_total_sum,
        db_caras: '',
        db_bonif: '',
        db_tarifa: '',
        db_costo: '',
      });
      continue;
    }

    const dbCaras = Number(db.caras) || 0;
    const dbBonif = Number(db.bonificacion) || 0;
    const dbTarifa = Number(db.tarifa_publica) || 0;
    const dbCosto = Number(db.costo) || 0;

    // Check if quantities match
    let expectedDbCaras, expectedDbBonif;
    if (isRenta) {
      expectedDbCaras = excel.caras_total;
      expectedDbBonif = 0;
    } else if (isBonif || isIntercambio || isCortesia) {
      expectedDbCaras = 0;
      expectedDbBonif = excel.caras_total;
    } else {
      expectedDbCaras = excel.caras_total;
      expectedDbBonif = 0;
    }

    const problems = [];

    // For RT articles: check caras match
    if (isRenta && dbCaras !== excel.caras_total) {
      problems.push(`Caras: excel=${excel.caras_total} db=${dbCaras}`);
    }
    // For BF/IN/CT: check bonif match
    if ((isBonif || isIntercambio || isCortesia) && dbBonif !== excel.caras_total) {
      problems.push(`Bonif: excel=${excel.caras_total} db=${dbBonif}`);
    }
    // Check tarifa
    if (isRenta && excel.tarifa > 0 && dbTarifa !== excel.tarifa) {
      problems.push(`Tarifa: excel=${excel.tarifa} db=${dbTarifa}`);
    }
    // Check monto total
    if (isRenta && excel.monto_total_sum > 0 && dbCosto !== excel.monto_total_sum) {
      problems.push(`Costo: excel=${excel.monto_total_sum} db=${dbCosto}`);
    }

    if (problems.length > 0) {
      results.push({
        aps_global: aps,
        articulo,
        campana: excel.campana,
        cliente: excel.cliente,
        problema: problems.join(' | '),
        excel_caras: excel.caras_total,
        excel_tarifa: excel.tarifa,
        excel_monto: excel.monto_total_sum,
        db_caras: dbCaras,
        db_bonif: dbBonif,
        db_tarifa: dbTarifa,
        db_costo: dbCosto,
      });
    }
  }

  // Also check: rows in DB that have articles that should be split (RT with bonif that should be BF/IN)
  // Find DB rows where there's an RT but no corresponding BF/IN in excel
  for (const [key, db] of Object.entries(dbGroups)) {
    const [aps, articulo] = key.split('|');
    if (!articulo) continue;
    const prefix = articulo.substring(0, 2);
    if (prefix !== 'RT') continue;

    const dbBonif = Number(db.bonificacion) || 0;
    if (dbBonif > 0) {
      // DB has bonif in an RT row - check if excel has separate BF/IN lines
      const bfKey = `${aps}|${articulo.replace('RT-', 'BF-')}`;
      const inKey = `${aps}|${articulo.replace('RT-', 'IN-')}`;
      const hasBF = excelGroups[bfKey];
      const hasIN = excelGroups[inKey];
      if (!hasBF && !hasIN && !results.find(r => r.aps_global === aps && r.articulo === articulo)) {
        results.push({
          aps_global: aps,
          articulo,
          campana: '',
          cliente: '',
          problema: `DB tiene bonif=${dbBonif} en fila RT pero excel no tiene BF/IN separados`,
          excel_caras: '',
          excel_tarifa: '',
          excel_monto: '',
          db_caras: Number(db.caras),
          db_bonif: dbBonif,
          db_tarifa: Number(db.tarifa_publica),
          db_costo: Number(db.costo),
        });
      }
    }
  }

  console.log(`\nMismatches found: ${results.length}`);

  // Write CSV
  const header = 'aps_global,articulo,campana,cliente,problema,excel_caras,excel_tarifa,excel_monto,db_caras,db_bonif,db_tarifa,db_costo';
  const csvRows = results.map(r =>
    [r.aps_global, r.articulo, `"${r.campana}"`, `"${r.cliente}"`, `"${r.problema}"`, r.excel_caras, r.excel_tarifa, r.excel_monto, r.db_caras, r.db_bonif, r.db_tarifa, r.db_costo].join(',')
  );
  fs.writeFileSync(OUTPUT_PATH, '\ufeff' + header + '\n' + csvRows.join('\n'), 'utf-8');
  console.log(`Output: ${OUTPUT_PATH}`);

  // Summary
  const noExiste = results.filter(r => r.problema === 'NO EXISTE EN BD');
  const carasMal = results.filter(r => r.problema.includes('Caras:') || r.problema.includes('Bonif:'));
  const tarifaMal = results.filter(r => r.problema.includes('Tarifa:'));
  const costoMal = results.filter(r => r.problema.includes('Costo:'));
  console.log(`\n--- Resumen ---`);
  console.log(`No existen en BD: ${noExiste.length}`);
  console.log(`Caras/Bonif mal: ${carasMal.length}`);
  console.log(`Tarifa mal: ${tarifaMal.length}`);
  console.log(`Costo mal: ${costoMal.length}`);

  await p.$disconnect();
}

main().catch(e => { console.error(e); process.exit(1); });
