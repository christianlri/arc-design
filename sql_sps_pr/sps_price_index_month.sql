-- This table extracts and maintains the efficiency metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 8.1
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_price_index_month
WITH
date_in AS (
  SELECT
    {%- if not params.backfill %}
    DATE(DATE_TRUNC(DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY), MONTH)) AS date_in
    {%- elif params.is_backfill_chunks_enabled %}
    DATE(DATE_TRUNC(CAST('{{ params.backfill_start_date }}' AS DATE), MONTH)) AS date_in
    {%- endif %}
)
, date_fin AS (
  SELECT
    {%- if not params.backfill %}
    CAST('{{ next_ds }}' AS DATE) AS date_fin
    {%- elif params.is_backfill_chunks_enabled %}
    CAST('{{ params.backfill_end_date }}' AS DATE) AS date_fin
    {%- endif %}
)
,pim AS (
    SELECT 
      product_id, 
      brand_owner_name, 
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.pim_product` 
    GROUP BY 1, 2
  ), 
  qc AS (
    SELECT 
      global_entity_id,
      country_code,
      sku AS sku_id,
      pim_product_id, 
      brand_name,
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` 
    WHERE REGEXP_CONTAINS(global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY 1, 2, 3, 4, 5
  ),
  products AS (
    SELECT 
      qc.global_entity_id,
      qc.country_code,
      qc.sku_id,
      COALESCE( NULLIF(LOWER(qc.brand_name), 'unbranded'), '_unknown_' ) AS brand_name,
      COALESCE( NULLIF(LOWER(pim.brand_owner_name), 'unbranded'), '_unknown_' ) AS brand_owner_name,
    FROM qc
    LEFT JOIN pim ON qc.pim_product_id = pim.product_id
  ),
  sku_supplier_mapping_for_price_index AS (
    SELECT
      global_entity_id,
      sku_id,
      CAST(supplier_id AS STRING) AS supplier_id,
      supplier_name
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product` 
    WHERE REGEXP_CONTAINS(global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY ALL
  ),
  principal_supplier AS (
    SELECT
      country_code,
      global_entity_id,
      CAST(supplier_id AS STRING) as supplier_id,
      ANY_VALUE(sup_id_parent) AS principal_supplier_id,
      supplier_name,
      division_type
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product` 
    WHERE REGEXP_CONTAINS(global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY ALL
  ),
  competitor_benchmark_base AS (
    SELECT
      cb.global_entity_id,
      CAST(DATE_TRUNC(cb.stamp_week, MONTH) AS STRING) AS price_index_month,
      CAST(CONCAT('Q', EXTRACT(QUARTER FROM cb.stamp_week), '-', EXTRACT(YEAR FROM cb.stamp_week)) AS STRING) AS price_index_quarter_year,
      cb.dmart_sku AS sku,
      COALESCE(cb.l1_master_category, '_unknown_' ) AS l1_master_category,
      COALESCE(cb.l2_master_category, '_unknown_' ) AS l2_master_category,
      COALESCE(cb.l3_master_category, '_unknown_' ) AS l3_master_category, 
      cb.median_bp_index,
      cb.sku_gpv_eur,
      p.brand_owner_name,
      p.brand_name,
    FROM `{{ params.project_id }}.{{ params.dataset.rl }}.competitor_benchmark_indices` AS cb
    LEFT JOIN products AS p
      ON cb.dmart_sku = p.sku_id
      AND cb.global_entity_id = p.global_entity_id
    WHERE TRUE
      AND REGEXP_CONTAINS(cb.global_entity_id, {{ params.param_global_entity_id }})
      AND (cb.stamp_week BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
  )
    SELECT
      cbb.global_entity_id,
      cbb.l1_master_category,
      cbb.l2_master_category,
      cbb.l3_master_category,
      cbb.brand_owner_name,
      cbb.brand_name,
      ps.principal_supplier_id,
      ssm.supplier_id,
      cbb.price_index_quarter_year,
      cbb.price_index_month,
      cbb.median_bp_index,
      cbb.sku_gpv_eur,
      CASE
        WHEN DATE_TRUNC(CAST(cbb.price_index_month AS DATE), MONTH) = DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH)
        THEN CAST('{{ next_ds }}' AS DATE)
          ELSE LAST_DAY(CAST(cbb.price_index_month AS DATE))
      END AS partition_month,
    FROM competitor_benchmark_base AS cbb
    LEFT JOIN sku_supplier_mapping_for_price_index AS ssm
      ON cbb.sku = ssm.sku_id
      AND cbb.global_entity_id = ssm.global_entity_id
    LEFT JOIN principal_supplier AS ps
      ON ssm.global_entity_id = ps.global_entity_id
      AND ssm.supplier_id = ps.supplier_id
    GROUP BY ALL
