DROP TABLE IF EXISTS core.servicios;

with tbl_main as (
SELECT
    gen_random_uuid() AS id,  -- UUID único por registro
    to_date(fecha, 'DD-MM-YY') AS fecha,
    TRIM(folio) AS folio,
    TRIM(cliente) AS cliente,
    TRIM(vehiculo) AS vehiculo,
    -- Normalización de nombres de mecánicos
    CASE 
        WHEN UPPER(TRIM(mecanico)) = 'RAFALE CARRILLO' THEN 'RAFAEL CARRILLO'
        WHEN UPPER(TRIM(mecanico)) = 'ENRIQUE' THEN 'ENRIQUE FLORES'
        WHEN UPPER(TRIM(mecanico)) = 'SLAVADOR' THEN 'SALVADOR'
        ELSE TRIM(mecanico)
    END AS mecanico,
    TRIM(movimiento) AS movimiento,
    TRIM(concepto) AS concepto,
    TRIM(descripcion) AS descripcion,
    NULLIF(
        TRIM(REPLACE(REPLACE(monto, '$', ''), ',', '')),
        ''
    )::NUMERIC AS monto
    ,concat(fecha,folio,cliente,vehiculo,mecanico,movimiento,concepto,descripcion,monto) as flag
FROM raw.servicios s
where concat(fecha,folio,cliente,vehiculo,mecanico,movimiento,concepto,descripcion,monto) is not null
)
select * from tbl_main 
where flag is not null
order by fecha desc;
;