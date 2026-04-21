-- This table extracts and maintains the financial metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No 11.1
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_shrinkage_month
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
tmp_sp_product AS (
 SELECT
   sp.global_entity_id,
  --  sp.country_code,
   sp.sku_id,
   COALESCE(CAST(sp.supplier_id AS STRING), '_unknown_') AS supplier_id,
   sp.sup_id_parent AS principal_supplier_id,
   COALESCE(sp.brand_name, '_unknown_') AS brand_name,
   COALESCE(sp.brand_owner_name, '_unknown_') AS brand_owner_name,
   sp.global_supplier_id,
   COALESCE(sp.level_one, '_unknown_') AS l1_master_category,
   COALESCE(sp.level_two, '_unknown_') AS l2_master_category,
   COALESCE(sp.level_three, '_unknown_') AS l3_master_category,
  --  MAX(sp.updated_at) AS last_updated,
 FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product` AS sp
 WHERE TRUE
  AND REGEXP_CONTAINS(sp.global_entity_id, {{ params.param_global_entity_id }})
 GROUP BY 1,2,3,4,5,6,7,8,9,10
),
 tmp_shrinkage AS (
    SELECT 
      global_entity_id,
      -- country_code,
      sku AS sku_id,
      CAST(supplier_id AS STRING ) AS supplier_id, 
      level_one_category AS l1_master_category,
      level_two_category AS l2_master_category,
      level_three_category AS l3_master_category,
      DATE_TRUNC(period, month) AS month,
      CAST(CONCAT('Q', EXTRACT(QUARTER FROM period), '-', EXTRACT(YEAR FROM period)) AS STRING) AS quarter_year,
      SUM(CASE WHEN full_stock_move_reason_raw = 'Store Expired' THEN movement_qty*wac_eod_eur ELSE 0 END) as spoilage_value,
      SUM(retail_revenue_eur*is_sales_considered) as retail_revenue,
    --   SUM(CASE WHEN full_stock_move_reason_raw = 'Store Expired' THEN movement_qty*wac_eod_eur ELSE 0 END)/
    -- NULLIF(SUM(retail_revenue_eur*is_sales_considered),0) AS spoilage_percent,
    from `{{ params.project_id }}.{{ params.dataset.rl }}.shrinkage_report` 
    WHERE TRUE
    AND REGEXP_CONTAINS(global_entity_id, {{ params.param_global_entity_id }})
    AND is_dmart
     AND (DATE_TRUNC(period, MONTH) BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
    GROUP BY 1,2,3,4,5,6,7,8
    order by 1,2
)
SELECT 
  te_sh.global_entity_id,
  -- COALESCE(te_sh.country_code, sp_exact.country_code) AS country_code,
  te_sh.sku_id,
  te_sh.month,
  te_sh.quarter_year,
  te_sh.spoilage_value,
  te_sh.retail_revenue,
  COALESCE(te_sh.supplier_id, sp_exact.supplier_id) AS supplier_id,
  -- sp_exact.supplier_name,
  sp_exact.brand_name,
  sp_exact.brand_owner_name,
  COALESCE(te_sh.l1_master_category, sp_exact.l1_master_category) AS l1_master_category,
  COALESCE(te_sh.l2_master_category, sp_exact.l2_master_category) AS l2_master_category,
  COALESCE(te_sh.l3_master_category, sp_exact.l3_master_category) AS l3_master_category,
  sp_exact.principal_supplier_id,
  CASE
    WHEN DATE_TRUNC(CAST(te_sh.month AS DATE), MONTH) = DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH)
    THEN CAST('{{ next_ds }}' AS DATE)
      ELSE LAST_DAY(CAST(te_sh.month AS DATE))
  END AS partition_month,
FROM tmp_shrinkage AS te_sh
JOIN tmp_sp_product AS sp_exact
  ON te_sh.sku_id = sp_exact.sku_id
  AND te_sh.global_entity_id = sp_exact.global_entity_id
