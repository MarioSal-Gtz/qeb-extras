# Respuesta a Estructura BI — QEB / Grupo IMU

**Fecha:** 7 de abril de 2026  
**De:** Equipo de Desarrollo QEB  
**Para:** Equipo de Business Intelligence  
**Asunto:** Implementación de vistas para integración con Power BI

---

## 1. Resumen de implementación

Se crearon las siguientes vistas en la base de datos de producción de QEB (MySQL 8 — DigitalOcean), siguiendo la estructura descrita en el documento *Estructura_BI_IMU*. Cada tabla técnica fue implementada como una vista independiente para facilitar la construcción de relaciones en Power BI.

### Vista: V_BI_APS_ESPECIFICOS

| Vista SQL | Tabla técnica SAP | Descripción |
|---|---|---|
| `V_ORDR` | ORDR | Encabezado de campaña / APS Global |
| `V_RDR1` | RDR1 | Líneas del pedido / Detalle de APS Global |
| `V_OSLP` | OSLP | Catálogo de asesores comerciales |
| `V_OUSR` | OUSR | Catálogo de usuarios / analistas comerciales |
| `V_OOCR` | OOCR | Reglas de distribución / centros de costo |

**Relaciones:**
- `V_ORDR.DocEntry` ↔ `V_RDR1.DocEntry` (encabezado → líneas)
- `V_ORDR.SlpCode` ↔ `V_OSLP.SlpCode` (campaña → asesor)
- `V_ORDR.UserSign` ↔ `V_OUSR.USERID` (campaña → usuario creador)
- `V_RDR1.OcrCode` ↔ `V_OOCR.OcrCode` (línea → centro de costo)

### Vista: V_OPORTUNIDADES_PROPUESTA

| Vista SQL | Tabla técnica SAP | Descripción |
|---|---|---|
| `V_OQUT` | OQUT | Encabezado de propuesta / cotización |
| `V_QUT1` | QUT1 | Líneas de propuesta / detalle |

**Relaciones:**
- `V_OQUT.DocEntry` ↔ `V_QUT1.DocEntry` (encabezado → líneas)

**Nota:** Las tablas ODRF / DRF1 (Borradores) no fueron implementadas dado que QEB no maneja un flujo de borradores equivalente al de SAP. Si se requieren, favor de indicar qué información se espera en ellas.

### Vista: V_OPORTUNIDADES

| Vista SQL | Tabla técnica SAP | Descripción |
|---|---|---|
| `V_OOPR` | OOPR | Oportunidades (solicitudes) |
| `V_OPR1` | OPR1 | Documentos relacionados a la oportunidad |
| `V_OOST` | OOST | Catálogo de fases de oportunidad |
| `V_OCLG` | OCLG | Actividades |

**Relaciones:**
- `V_OOPR.OpprId` ↔ `V_OPR1.OpprId` (oportunidad → documentos)
- `V_OOPR.StepLast` ↔ `V_OOST.Descript` (oportunidad → fase)

---

## 2. Campos pendientes de definición

Los siguientes campos fueron incluidos en las vistas con valor `NULL` debido a que actualmente no se generan ni almacenan en QEB. Solicitamos su apoyo para determinar si son indispensables para los reportes y, de ser así, cómo deberían ser poblados.

### V_ORDR / V_OQUT (Encabezados)

| Campo | Nombre coloquial | Observación |
|---|---|---|
| `DocTime` | Hora de creación | QEB registra fecha de creación pero no hora como campo independiente. ¿Se requiere? En caso afirmativo, se puede derivar del timestamp de creación. |

### V_RDR1 / V_QUT1 (Líneas)

| Campo | Nombre coloquial | Observación |
|---|---|---|
| `U_Cod_Sitio` | Código de sitio / plaza | QEB almacena el nombre de la plaza (texto), no un código numérico. ¿Existe un catálogo de códigos de sitio que debamos mapear? |
| `OcrCode` | Centro de costo | QEB no almacena reglas de distribución. Este dato se obtiene del artículo en SAP (`U_IMU_OcrCode`). ¿Se requiere que QEB lo almacene y exponga, o se puede resolver del lado de BI cruzando con el catálogo de artículos? |
| `TrgetEntry` | Documento destino | Referencia al DeliveryNote generado en SAP. QEB genera el DeliveryNote vía API pero actualmente no almacena el folio de respuesta de SAP. ¿Se requiere que QEB lo almacene? |

### V_OOPR (Oportunidades)

| Campo | Nombre coloquial | Observación |
|---|---|---|
| `CloseDate` | Fecha de cierre | QEB no maneja un concepto de cierre de oportunidad. ¿Se puede utilizar la fecha de aprobación de la campaña como equivalente? |
| `CloPrcnt` | Porcentaje de cierre | QEB no maneja probabilidad de cierre. ¿Es indispensable para los reportes? |

### Tablas vacías

| Vista | Tabla SAP | Observación |
|---|---|---|
| `V_OOCR` | OOCR | Sin datos. ¿Se requiere un catálogo de centros de costo? De ser así, ¿de dónde se obtiene? |
| `V_OCLG` | OCLG | Sin datos. QEB no maneja actividades tipo CRM. ¿Es indispensable? |
| `V_ODRF` / `V_DRF1` | ODRF / DRF1 | No implementadas. QEB no maneja borradores. ¿Se requieren? |

---

## 3. Conexión a Power BI

Las vistas están disponibles para consulta directa desde Power BI con los siguientes datos de conexión:

| Parámetro | Valor |
|---|---|
| Servidor | `qeb-mysql-prod-do-user-32408772-0.g.db.ondigitalocean.com` |
| Puerto | `25060` |
| Base de datos | `u658050396_QEB` |
| Usuario | `doadmin` |
| SSL | Requerido |

**Requisito:** La dirección IP desde la cual se conecte Power BI debe ser registrada como *Trusted Source* en la configuración de la base de datos. Favor de proporcionar la IP para darla de alta.

---

## 4. Consideraciones

- Las vistas se actualizan en tiempo real (no son snapshots). Cualquier cambio en QEB se refleja inmediatamente.
- Las vistas son de solo lectura; no modifican datos en la base de datos.
- Si se requiere algún campo adicional o un formato diferente de presentación, quedo a la orden para ajustar las vistas.
