-- sql/03_mart_servicios_summary.sql
DROP VIEW IF EXISTS mart.servicios_summary;
CREATE VIEW mart.servicios_summary AS
WITH ventas_ref AS (
  SELECT folio, descripcion, SUM(monto) AS venta_ref
  FROM core.servicios
  WHERE UPPER(concepto)='REFACCION' AND UPPER(movimiento)='VENTA'
  GROUP BY folio, descripcion
),
compras_ref AS (
  SELECT folio, descripcion, SUM(monto) AS costo_ref
  FROM core.servicios
  WHERE UPPER(concepto)='REFACCION' AND UPPER(movimiento)='COMPRA'
  GROUP BY folio, descripcion
),
ventas_sin_compra AS (
  SELECT v.folio, COUNT(*) AS piezas_sin_compra
  FROM ventas_ref v
  LEFT JOIN compras_ref c USING (folio, descripcion)
  WHERE c.descripcion IS NULL
  GROUP BY v.folio
),
compras_sin_venta AS (
  SELECT c.folio, COUNT(*) AS piezas_sin_venta
  FROM compras_ref c
  LEFT JOIN ventas_ref v USING (folio, descripcion)
  WHERE v.descripcion IS NULL
  GROUP BY c.folio
),
agg_ref AS (
  SELECT
    COALESCE(v.folio, c.folio) AS folio,
    SUM(COALESCE(v.venta_ref,0)) AS venta_refacciones,
    SUM(COALESCE(c.costo_ref,0)) AS costo_refacciones
  FROM ventas_ref v
  FULL JOIN compras_ref c USING (folio, descripcion)
  GROUP BY 1
),
agg_mo AS (
  SELECT folio,
         SUM(CASE WHEN UPPER(concepto)='MANO DE OBRA' AND UPPER(movimiento)='VENTA' THEN monto ELSE 0 END) AS venta_mano_obra
  FROM core.servicios
  GROUP BY folio
),
meta AS (
  SELECT folio,
         MIN(fecha) AS fecha_min,
         MAX(fecha) AS fecha_max,
         MAX(cliente)  AS cliente,
         MAX(vehiculo) AS vehiculo,
         MAX(mecanico) AS mecanico_ref
  FROM core.servicios
  GROUP BY folio
)
SELECT
  m.folio,
  m.cliente,
  m.vehiculo,
  m.mecanico_ref AS mecanico,
  m.fecha_min    AS fecha_inicio,
  m.fecha_max    AS fecha_fin,
  COALESCE(r.venta_refacciones,0)                         AS venta_refacciones,
  COALESCE(r.costo_refacciones,0)                         AS costo_refacciones,
  COALESCE(r.venta_refacciones,0) - COALESCE(r.costo_refacciones,0) AS utilidad_refacciones,
  COALESCE(mo.venta_mano_obra,0)                          AS venta_mano_obra,
  COALESCE(mo.venta_mano_obra,0) * 0.30                   AS pago_mecanico,
  COALESCE(mo.venta_mano_obra,0) * 0.70                   AS utilidad_mano_obra,
  COALESCE(r.venta_refacciones,0) + COALESCE(mo.venta_mano_obra,0)  AS total_venta,
  COALESCE(r.costo_refacciones,0) + (COALESCE(mo.venta_mano_obra,0) * 0.30) AS total_costos,
  (COALESCE(r.venta_refacciones,0) - COALESCE(r.costo_refacciones,0))
  + (COALESCE(mo.venta_mano_obra,0) * 0.70)               AS utilidad_total,
  CASE WHEN (COALESCE(r.venta_refacciones,0) + COALESCE(mo.venta_mano_obra,0)) > 0
       THEN ((COALESCE(r.venta_refacciones,0) - COALESCE(r.costo_refacciones,0))
             + (COALESCE(mo.venta_mano_obra,0) * 0.70))
            / (COALESCE(r.venta_refacciones,0) + COALESCE(mo.venta_mano_obra,0))
       ELSE 0 END                                         AS "%utilidad",
  COALESCE(vsc.piezas_sin_compra,0)                       AS piezas_ref_sin_compra,
  COALESCE(csv.piezas_sin_venta,0)                        AS piezas_ref_sin_venta
FROM meta m
LEFT JOIN agg_ref r  ON r.folio  = m.folio
LEFT JOIN agg_mo mo  ON mo.folio = m.folio
LEFT JOIN ventas_sin_compra vsc ON vsc.folio = m.folio
LEFT JOIN compras_sin_venta csv ON csv.folio = m.folio
ORDER BY m.fecha_min, m.folio;