--This table aggregate all product-related information, including country, SKU, supplier, and warehouse-level data, essential for rebate calculations.
{%- if not params.backfill %}
  {% 
    set param_month = (
      data_interval_end.replace(day=1) - macros.dateutil.relativedelta.relativedelta(months=1)
    ).strftime('%Y-%m-%d') 
  %}
{%- elif params.is_backfill_chunks_enabled %}
  {% set param_month = params.backfill_start_date %}
{%- endif %}

WITH qc_stock_movements AS (
  SELECT
    sm.country_code,
    sm.global_entity_id,
    sm.warehouse_id,
    DATE_TRUNC(sm.stock_date_lt, month) AS date_localtime_at,
    sm.sku,
    smd.wac_lc,
    smd.full_stock_move_reason,
    SUM(smd.quantity) AS quantity,
  FROM
    `{{ params.project_id }}.{{ params.dataset.curated_data_shared }}.qc_stock_movements` AS sm
  LEFT JOIN
    UNNEST (sm.stock_movements) AS smd
  WHERE
    TRUE
    AND (DATE_TRUNC(sm.stock_date_lt, month) BETWEEN DATE_ADD(DATE_TRUNC('{{ param_month }}', month),INTERVAL 0 MONTH) 
    AND DATE_ADD(DATE_TRUNC('{{ param_month }}', month), INTERVAL 0 MONTH)) 
    AND REGEXP_CONTAINS(sm.country_code, {{ params.stock_param_country_code }})
    /*only the reason 'supplier return' and 'other' should be considered*/
    AND REGEXP_CONTAINS(LOWER(smd.full_stock_move_reason), r'supplier return|other')
    AND smd.quantity IS NOT NULL 
    AND NOT REGEXP_CONTAINS(sm.warehouse_id, {{ params.exclude_service_warehouses_param }}) 
  GROUP BY ALL
),
products AS (
  SELECT
    p.country_code,
    p.global_entity_id,
    p.sku,
    p.sup_id,
    p.warehouse_id,
    p.is_dmart,
    p.store_id,
    p.vat_rate,
    /*p.start_date*/
  FROM
    `{{ params.project_id }}.{{ params.dataset.cl }}.rb_products`AS p
  WHERE
    TRUE
  GROUP BY ALL
),
warehouses_v2 AS (
  SELECT 
    wv2.country_code, 
    wv2.global_entity_id,
    wv2.warehouse_id, 
    wv2.name AS warehouse_name,
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.warehouses_v2` AS wv2
  GROUP BY ALL
  ORDER BY 1,2,3,4
),
srm_excise_tax AS (
    SELECT 
    gset.country_code,
    gset.sku,
    gset.tax AS excise_tax,
    gset.start_date, 
    gset.end_date,
    FROM `{{ params.project_id }}.{{ params.dataset.dl }}.gsheet_gsh_srm_excise_tax` AS gset
    GROUP BY ALL
),
sources AS (
    SELECT s.timezone,
    s.country_code
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sources` AS s
    GROUP BY ALL
)

SELECT 
  'stock_movement' AS source_table,
  sm.country_code,
  sm.global_entity_id,
  p.sup_id,
  sm.warehouse_id,
  wv2.warehouse_name,
  sm.date_localtime_at AS received_local_time,
  sm.sku,
  sm.wac_lc AS gross_cost_without_vat,
  sm.wac_lc AS gross_cost,
  p.vat_rate,
  sm.full_stock_move_reason,
  CASE
    WHEN p.is_dmart IS TRUE THEN 'DMART'
  ELSE
    'DISTRIBUTION CENTER'
  END
  AS warehouse_type,
  gset.excise_tax,
  /*p.start_date,*/
  /*p.store_id,*/
  CASE
    WHEN (REGEXP_CONTAINS( LOWER(sm.full_stock_move_reason), 'supplier return')) THEN 'Supplier Return'
    WHEN (REGEXP_CONTAINS( LOWER(sm.full_stock_move_reason), 'other')
      AND p.is_dmart IS FALSE
      AND REGEXP_CONTAINS( LOWER(sm.country_code), {{ params.fixing_po_country_code }})) THEN 'Fixing Po'
    ELSE
      NULL
  END AS fixing_condition,  
  sm.full_stock_move_reason AS reason,
  SUM(sm.quantity) * (-1) AS returned_quantity,
  DATE('{{ param_month }}') AS month
FROM qc_stock_movements AS sm
LEFT JOIN warehouses_v2 AS wv2
ON sm.country_code = wv2.country_code
  AND sm.warehouse_id = wv2.warehouse_id
JOIN products AS p
ON sm.country_code = p.country_code
  AND sm.sku = p.sku
  AND sm.warehouse_id = p.warehouse_id
LEFT JOIN sources AS s ON sm.country_code = s.country_code
LEFT JOIN srm_excise_tax AS gset 
ON sm.sku = gset.sku
  AND sm.country_code = gset.country_code
  AND (DATETIME (TIMESTAMP(sm.date_localtime_at), s.timezone) BETWEEN DATE_TRUNC(gset.start_date, day) AND DATE_TRUNC(gset.end_date, day))
WHERE TRUE
AND  (REGEXP_CONTAINS(LOWER(sm.full_stock_move_reason),'supplier return' )
  OR (REGEXP_CONTAINS(LOWER(sm.full_stock_move_reason),'other' )
    AND p.is_dmart IS FALSE
    AND REGEXP_CONTAINS( LOWER(sm.country_code), {{ params.fixing_po_country_code }})
  )
)
GROUP BY ALL
