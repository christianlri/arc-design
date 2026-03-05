-- This table extracts and maintains the days payable metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 10.1
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_listed_sku_month

WITH
date_in AS (
  SELECT
    {%- if not params.backfill %}
    DATE(DATE_TRUNC(DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY), MONTH)) AS date_in
    {%- elif params.is_backfill_chunks_enabled %}
    DATE(DATE_TRUNC(CAST('{{ params.backfill_start_date }}' AS DATE), MONTH)) AS date_in
    {%- endif %} 
 ),
 date_fin AS (
  SELECT
    {%- if not params.backfill %}
    CAST('{{ next_ds }}' AS DATE) AS date_fin
    {%- elif params.is_backfill_chunks_enabled %}
    CAST('{{ params.backfill_end_date }}' AS DATE) AS date_fin
    {%- endif %}
  ),
  sps_product AS (
    SELECT
      sp.global_entity_id,
      sp.sku_id,
      COALESCE(CAST(sp.supplier_id AS STRING), '_unknown_') AS supplier_id,
      sp.sup_id_parent AS principal_supplier_id,
      -- CASE WHEN CAST(sp.supplier_id AS STRING) = sp.sup_id_parent THEN TRUE END AS is_sup_id_parent,
      -- sp.is_sup_id_parent,
      -- sp.division_type,
      COALESCE( NULLIF(LOWER(sp.brand_name), 'unbranded'), '_unknown_' ) AS brand_name,
      COALESCE( NULLIF(LOWER(sp.brand_owner_name), 'unbranded'), '_unknown_' ) AS brand_owner_name,
      sp.global_supplier_id,
      COALESCE(sp.level_one, '_unknown_') AS l1_master_category,
      COALESCE(sp.level_two, '_unknown_') AS l2_master_category,
      COALESCE(sp.level_three, '_unknown_') AS l3_master_category,
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product` AS sp
    WHERE TRUE
      AND REGEXP_CONTAINS(sp.global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  ),
  sps_listed_sku AS (
    SELECT 
      DATE_TRUNC(stock_date, MONTH) AS month, 
      CAST(CONCAT('Q', EXTRACT(QUARTER FROM stock_date), '-', EXTRACT(YEAR FROM stock_date)) AS STRING) AS quarter_year,
      global_entity_id, 
      -- warehouse_id, 
      sku, 
      is_listed,
    FROM `{{ params.project_id }}.{{ params.dataset.rl }}._stock_daily_listed`
    WHERE TRUE
      AND REGEXP_CONTAINS(global_entity_id, {{ params.param_global_entity_id }})
      AND (DATE_TRUNC(stock_date, MONTH) BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
    AND is_listed
    GROUP BY 1, 2, 3, 4, 5
  )
  SELECT 
    sls.month, 
    sls.quarter_year,
    sls.global_entity_id,

    sp.supplier_id,
    sp.principal_supplier_id,
    -- sp.is_sup_id_parent,
    -- sp.division_type,

    sp.brand_name,
    sp.brand_owner_name,

    sp.l1_master_category,
    sp.l2_master_category,
    sp.l3_master_category,
    sls.sku AS sku_id,
    -- COUNT(DISTINCT sls.sku) AS listed_skus
    CASE
    WHEN DATE_TRUNC(CAST(sls.month AS DATE), MONTH) = DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH)
    THEN CAST('{{ next_ds }}' AS DATE)
      ELSE LAST_DAY(CAST(sls.month AS DATE))
  END AS partition_month,
  FROM sps_listed_sku AS sls
  LEFT JOIN sps_product AS sp 
    ON sp.global_entity_id = sls.global_entity_id
    AND sp.sku_id = sls.sku
  WHERE TRUE
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
