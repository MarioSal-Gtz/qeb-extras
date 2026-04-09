/**
 * Migration Script: Cat 08 Fix + New Campaigns
 *
 * 3 operations:
 * 1. CARGADA EN QEB: correct solicitudCaras (split RT into RT+BF+IN)
 * 2. NUEVA (existing campaign): add cat 8 caras, extend dates
 * 3. NUEVA (totally new): create solicitud+propuesta+cotizacion+campania+solicitudCaras
 *
 * Usage: DRY_RUN=1 node migrate_cat08_fix.js  (preview only)
 *        DRY_RUN=0 node migrate_cat08_fix.js  (execute)
 */

const fs = require('fs');
const { PrismaClient } = require('@prisma/client');

const DRY_RUN = process.env.DRY_RUN !== '0';
const CSV_PATH = 'C:/Users/Mario/Downloads/Cat08_listo_para_migrar.csv';
const CAT8_INICIO = '2026-04-14';
const CAT8_FIN = '2026-04-27';

const CIUDAD_ESTADO_MAP = {
  "ACAPULCO DE JUÁREZ": "Guerrero",
  "GUADALAJARA": "Jalisco", "ZAPOPAN": "Jalisco",
  "MONTERREY": "Nuevo León",
  "PUEBLA": "Puebla",
  "LEÓN": "Guanajuato",
  "MÉRIDA": "Yucatán",
  "TIJUANA": "Baja California",
  "CULIACÁN": "Sinaloa", "MAZATLÁN": "Sinaloa",
  "MIGUEL HIDALGO": "Ciudad de México",
  "OAXACA DE JUÁREZ": "Oaxaca",
  "PACHUCA DE SOTO": "Hidalgo",
  "BOCA DEL RÍO": "Veracruz",
  "PUERTO VALLARTA": "Jalisco",
  "TOLUCA": "Estado de México",
  "QUERÉTARO": "Querétaro",
};

function parseCSV(path) {
  const text = fs.readFileSync(path, 'utf-8');
  const lines = text.split('\n').filter(l => l.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const f = []; let c = '', q = false;
    for (const ch of lines[i]) { if (ch === '"') q = !q; else if (ch === ',' && !q) { f.push(c.trim()); c = ''; } else c += ch; }
    f.push(c.trim());
    if (!f[7]) continue; // skip rows without articulo
    rows.push({
      campana: f[0], cliente: f[1], operacion: f[2],
      tarifa: parseFloat(f[4]) || 0, aps: f[5], cuic: f[6],
      articulo: f[7], vendedor: f[8], ciudad: f[18],
      userId: parseInt(f[22]) || null, userName: f[23],
      asignadoStr: f[24], asignadoIds: f[25],
      formato: f[26], estatus: f[29],
    });
  }
  return rows;
}

async function main() {
  console.log(DRY_RUN ? '\n=== DRY RUN MODE (no changes) ===' : '\n=== EXECUTING MIGRATION ===');

  const p = new PrismaClient();
  const rows = parseCSV(CSV_PATH);
  console.log('CSV rows:', rows.length);

  // Group by APS + articulo
  const campaigns = {};
  for (const row of rows) {
    if (!campaigns[row.aps]) {
      campaigns[row.aps] = { ...row, arts: {} };
    }
    if (!campaigns[row.aps].arts[row.articulo]) {
      campaigns[row.aps].arts[row.articulo] = { count: 0, tarifa: row.tarifa, operacion: row.operacion, ciudades: new Set(), formato: row.formato };
    }
    campaigns[row.aps].arts[row.articulo].count++;
    campaigns[row.aps].arts[row.articulo].ciudades.add(row.ciudad);
  }

  // Get existing data
  const existingProps = new Set((await p.propuesta.findMany({ select: { id: true } })).map(x => x.id));
  const existingCarasRaw = await p.$queryRawUnsafe(
    "SELECT sc.id, sc.idquote, sc.articulo, sc.caras, sc.bonificacion FROM solicitudCaras sc WHERE sc.inicio_periodo >= '2026-04-14' AND sc.inicio_periodo <= '2026-04-27'"
  );
  const existingCaras = {};
  for (const c of existingCarasRaw) {
    existingCaras[c.idquote + '|' + c.articulo] = c;
  }

  // Get cliente IDs by CUIC
  const clientes = await p.cliente.findMany({ select: { id: true, CUIC: true } });
  const cuicToId = {};
  clientes.forEach(c => { if (c.CUIC) cuicToId[c.CUIC] = c.id; });

  const stats = { carasUpdated: 0, carasInserted: 0, campaignsExtended: 0, campaignsCreated: 0, errors: [] };

  for (const [aps, camp] of Object.entries(campaigns)) {
    const apsNum = parseInt(aps);
    const propExists = existingProps.has(apsNum);
    const isCargada = camp.estatus === 'CARGADA EN QEB';

    try {
      if (isCargada && propExists) {
        // === CASE 1: CARGADA EN QEB - correct existing caras ===
        for (const [art, data] of Object.entries(camp.arts)) {
          const key = aps + '|' + art;
          const existing = existingCaras[key];
          const prefix = art.substring(0, 2).toUpperCase();
          const isBonifType = prefix === 'BF' || prefix === 'IN' || prefix === 'CT' || prefix === 'CF';
          const ciudadStr = [...data.ciudades].sort().join(', ');
          const estadoStr = [...new Set([...data.ciudades].map(c => CIUDAD_ESTADO_MAP[c] || c))].sort().join(', ');
          const isTarifaZero = prefix === 'CT' || prefix === 'IN' || prefix === 'BF' || prefix === 'CF';
          const tipo = (art.includes('DIG') || art.includes('PRG') || art.includes('KCS')) ? 'Digital' : 'Tradicional';

          if (existing) {
            // Update existing cara
            const newCaras = isBonifType ? 0 : data.count;
            const newBonif = isBonifType ? data.count : 0;
            const newTarifa = isTarifaZero ? 0 : data.tarifa;
            const newCosto = isTarifaZero ? 0 : (data.tarifa * data.count);

            if (Number(existing.caras) !== newCaras || Number(existing.bonificacion) !== newBonif) {
              console.log(`  UPDATE ${key}: caras ${existing.caras}->${newCaras}, bonif ${existing.bonificacion}->${newBonif}`);
              if (!DRY_RUN) {
                await p.solicitudCaras.update({
                  where: { id: existing.id },
                  data: { caras: newCaras, bonificacion: newBonif.toString(), tarifa_publica: newTarifa, costo: newCosto }
                });
              }
              stats.carasUpdated++;
            }
          } else {
            // Insert new cara (BF/IN that didn't exist)
            console.log(`  INSERT ${key}: ${isBonifType ? 'bonif' : 'caras'}=${data.count}, tarifa=${data.tarifa}`);
            if (!DRY_RUN) {
              await p.solicitudCaras.create({
                data: {
                  idquote: aps, ciudad: ciudadStr, estados: estadoStr,
                  tipo, flujo: 'Ambos', caras: isBonifType ? 0 : data.count,
                  nivel_socioeconomico: 'Todos', formato: data.formato,
                  costo: isTarifaZero ? 0 : (data.tarifa * data.count),
                  tarifa_publica: isTarifaZero ? 0 : data.tarifa,
                  inicio_periodo: new Date(CAT8_INICIO), fin_periodo: new Date(CAT8_FIN),
                  caras_flujo: 0, caras_contraflujo: 0,
                  articulo: art, bonificacion: (isBonifType ? data.count : 0).toString(),
                  cortesia: (prefix === 'CT') ? 1 : 0,
                }
              });
            }
            stats.carasInserted++;
          }
        }

      } else if (!isCargada && propExists) {
        // === CASE 2: NUEVA but campaign exists - extend to cat 8 ===
        // Check if caras already exist for this APS in cat 8
        const hasAnyCat8 = Object.keys(camp.arts).some(art => existingCaras[aps + '|' + art]);

        for (const [art, data] of Object.entries(camp.arts)) {
          const key = aps + '|' + art;
          if (existingCaras[key]) {
            // Already exists in cat 8, skip
            continue;
          }

          const prefix = art.substring(0, 2).toUpperCase();
          const isBonifType = prefix === 'BF' || prefix === 'IN' || prefix === 'CT' || prefix === 'CF';
          const isTarifaZero = prefix === 'CT' || prefix === 'IN' || prefix === 'BF' || prefix === 'CF';
          const ciudadStr = [...data.ciudades].sort().join(', ');
          const estadoStr = [...new Set([...data.ciudades].map(c => CIUDAD_ESTADO_MAP[c] || c))].sort().join(', ');
          const tipo = (art.includes('DIG') || art.includes('PRG') || art.includes('KCS')) ? 'Digital' : 'Tradicional';

          console.log(`  INSERT (extend) ${key}: ${isBonifType ? 'bonif' : 'caras'}=${data.count}`);
          if (!DRY_RUN) {
            await p.solicitudCaras.create({
              data: {
                idquote: aps, ciudad: ciudadStr, estados: estadoStr,
                tipo, flujo: 'Ambos', caras: isBonifType ? 0 : data.count,
                nivel_socioeconomico: 'Todos', formato: data.formato,
                costo: isTarifaZero ? 0 : (data.tarifa * data.count),
                tarifa_publica: isTarifaZero ? 0 : data.tarifa,
                inicio_periodo: new Date(CAT8_INICIO), fin_periodo: new Date(CAT8_FIN),
                caras_flujo: 0, caras_contraflujo: 0,
                articulo: art, bonificacion: (isBonifType ? data.count : 0).toString(),
                cortesia: (prefix === 'CT') ? 1 : 0,
              }
            });
          }
          stats.carasInserted++;
        }

        // Extend campaign dates to include cat 8
        const cat8Fin = new Date(CAT8_FIN);
        console.log(`  EXTEND campaign ${aps} fecha_fin -> ${CAT8_FIN}`);
        if (!DRY_RUN) {
          // Update campania
          const campania = await p.campania.findFirst({ where: { cotizacion_id: { in: (await p.cotizacion.findMany({ where: { id_propuesta: apsNum }, select: { id: true } })).map(c => c.id) } } });
          if (campania && (!campania.fecha_fin || campania.fecha_fin < cat8Fin)) {
            await p.campania.update({ where: { id: campania.id }, data: { fecha_fin: cat8Fin } });
          }
          // Update cotizacion
          const cot = await p.cotizacion.findFirst({ where: { id_propuesta: apsNum } });
          if (cot && (!cot.fecha_fin || cot.fecha_fin < cat8Fin)) {
            await p.cotizacion.update({ where: { id: cot.id }, data: { fecha_fin: cat8Fin } });
          }
        }
        stats.campaignsExtended++;

      } else if (!isCargada && !propExists) {
        // === CASE 3: Totally new campaign ===
        const clienteId = cuicToId[parseInt(camp.cuic)];
        if (!clienteId) {
          stats.errors.push(`APS ${aps}: no cliente for CUIC ${camp.cuic}`);
          continue;
        }

        console.log(`  CREATE new campaign: APS=${aps} "${camp.campana}" cliente=${clienteId}`);

        if (!DRY_RUN) {
          // Create solicitud
          const solicitud = await p.solicitud.create({
            data: {
              fecha: new Date(), descripcion: camp.campana, presupuesto: 0,
              notas: 'Migración Cat 08', cliente_id: clienteId,
              usuario_id: camp.userId || 1, status: 'Atendida',
              nombre_usuario: camp.userName || 'Sistema',
              asignado: camp.asignadoStr || '', id_asignado: camp.asignadoIds || '',
              cuic: camp.cuic, razon_social: camp.cliente, IMU: 0,
            }
          });

          // Create propuesta
          const propuesta = await p.propuesta.create({
            data: {
              id: apsNum,
              solicitud_id: solicitud.id, cliente_id: clienteId,
              status: 'Aprobada', inversion: 0,
              descripcion: camp.campana, notas: 'Migración Cat 08',
              fecha: new Date(), asignado: camp.asignadoStr || '',
              id_asignado: camp.asignadoIds || '',
              comentario_cambio_status: '', articulo: Object.keys(camp.arts)[0] || '',
            }
          });

          // Create cotizacion
          const cotizacion = await p.cotizacion.create({
            data: {
              user_id: camp.userId || 1, clientes_id: clienteId,
              nombre_campania: camp.campana, numero_caras: Object.values(camp.arts).reduce((a, b) => a + b.count, 0),
              fecha_inicio: new Date(CAT8_INICIO), fecha_fin: new Date(CAT8_FIN),
              observaciones: '', bonificacion: 0, descuento: 0, precio: 0,
              contacto: camp.userName || '', status: 'Aprobada',
              id_propuesta: apsNum, articulo: Object.keys(camp.arts)[0] || '',
              tipo_periodo: 'catorcena',
            }
          });

          // Create campania
          await p.campania.create({
            data: {
              cliente_id: clienteId, nombre: camp.campana,
              fecha_inicio: new Date(CAT8_INICIO), fecha_fin: new Date(CAT8_FIN),
              total_caras: Object.values(camp.arts).reduce((a, b) => a + b.count, 0).toString(),
              bonificacion: 0, status: 'Aprobada',
              cotizacion_id: cotizacion.id,
              articulo: Object.keys(camp.arts)[0] || '',
            }
          });

          // Create solicitudCaras
          for (const [art, data] of Object.entries(camp.arts)) {
            const prefix = art.substring(0, 2).toUpperCase();
            const isBonifType = prefix === 'BF' || prefix === 'IN' || prefix === 'CT' || prefix === 'CF';
            const isTarifaZero = prefix === 'CT' || prefix === 'IN' || prefix === 'BF' || prefix === 'CF';
            const ciudadStr = [...data.ciudades].sort().join(', ');
            const estadoStr = [...new Set([...data.ciudades].map(c => CIUDAD_ESTADO_MAP[c] || c))].sort().join(', ');
            const tipo = (art.includes('DIG') || art.includes('PRG') || art.includes('KCS')) ? 'Digital' : 'Tradicional';

            await p.solicitudCaras.create({
              data: {
                idquote: aps, ciudad: ciudadStr, estados: estadoStr,
                tipo, flujo: 'Ambos', caras: isBonifType ? 0 : data.count,
                nivel_socioeconomico: 'Todos', formato: data.formato,
                costo: isTarifaZero ? 0 : (data.tarifa * data.count),
                tarifa_publica: isTarifaZero ? 0 : data.tarifa,
                inicio_periodo: new Date(CAT8_INICIO), fin_periodo: new Date(CAT8_FIN),
                caras_flujo: 0, caras_contraflujo: 0,
                articulo: art, bonificacion: (isBonifType ? data.count : 0).toString(),
                cortesia: (prefix === 'CT') ? 1 : 0,
              }
            });
          }
        }
        stats.campaignsCreated++;
        for (const art of Object.keys(camp.arts)) stats.carasInserted++;
      }
    } catch (err) {
      stats.errors.push(`APS ${aps}: ${err.message}`);
      console.error(`  ERROR APS ${aps}:`, err.message);
    }
  }

  console.log('\n=== RESUMEN ===');
  console.log('Caras actualizadas (corregidas):', stats.carasUpdated);
  console.log('Caras insertadas (nuevas):', stats.carasInserted);
  console.log('Campañas extendidas a cat 8:', stats.campaignsExtended);
  console.log('Campañas nuevas creadas:', stats.campaignsCreated);
  console.log('Errores:', stats.errors.length);
  if (stats.errors.length > 0) stats.errors.forEach(e => console.log('  ⚠', e));
  console.log(DRY_RUN ? '\n(DRY RUN - no se hicieron cambios)' : '\n✓ MIGRACIÓN COMPLETADA');

  await p.$disconnect();
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
