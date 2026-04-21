-- This table extracts and maintains the financial metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No 12.1
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_delivery_costs_month
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
rdvr_dc_cmt AS (
  SELECT 
    rdc.period AS month, 
    rdc.country AS country_name, 
    rdc.cost_local_currency AS cost_local, 
    rdc.cost_euro
  FROM `dh-darkstores-stg.dev_dmart.rdvr_dc_cmt` AS rdc
),
scm_dc_centralization AS (
  -- Step 1: Aggregate raw data to the Month + SKU level
  SELECT
    DATE_TRUNC(sdc.inbound_date, MONTH) AS month,
    CAST(CONCAT('Q', EXTRACT(QUARTER FROM sdc.inbound_date), '-', EXTRACT(YEAR FROM sdc.inbound_date)) AS STRING) AS quarter_year,
    sdc.global_entity_id,
    sdc.country_code,
    sdc.common_name AS country_name,
    -- sdc.warehouse_id,
    -- sdc.supplier_id,
    sdc.sku AS sku_id,
    SUM(sdc.inbounded_quantity) AS total_inbounded_quantity
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.scm_dc_centralization` AS sdc
  WHERE TRUE
    AND REGEXP_CONTAINS(sdc.country_code, {{ params.param_country_code }})
    AND sdc.inbound_type = 'DC to Dmart'
    AND (DATE_TRUNC(sdc.inbound_date, MONTH) BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
  GROUP BY 1, 2, 3, 4, 5, 6
),
scm_dc_centralization_share AS (
  -- Step 2: Calculate the shares based on the already-grouped data
  SELECT
    sdc_base.*,
    -- Total per country for that month
    SUM(sdc_base.total_inbounded_quantity) OVER (PARTITION BY sdc_base.month, sdc_base.country_code
    ) AS total_country_month_qty,
    -- Share of SKU over country total
    SAFE_DIVIDE(
      sdc_base.total_inbounded_quantity, 
      SUM(sdc_base.total_inbounded_quantity) OVER (PARTITION BY sdc_base.month, sdc_base.country_code
     )
    ) AS sku_share_of_country_total
  FROM scm_dc_centralization AS sdc_base
)
  SELECT 
    sdcs.month, 
    sdcs.quarter_year,
    sdcs.global_entity_id,
    sdcs.country_code,
    sdcs.sku_id,
    sp.brand_name,
    sp.brand_owner_name,
    sp.l1_master_category,
    sp.l2_master_category,
    sp.l3_master_category,
    sp.supplier_id,
    sp.principal_supplier_id,
    sdcs.total_inbounded_quantity * rdrv.cost_euro AS allocated_delivery_cost_eur,
    sdcs.total_inbounded_quantity * rdrv.cost_local AS allocated_delivery_cost_local,
  CASE
    WHEN DATE_TRUNC(CAST(sdcs.month AS DATE), MONTH) = DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH)
    THEN CAST('{{ next_ds }}' AS DATE)
      ELSE LAST_DAY(CAST(sdcs.month AS DATE))
  END AS partition_month,
  FROM scm_dc_centralization_share AS sdcs
  LEFT JOIN sps_product AS sp 
    ON sdcs.sku_id = sp.sku_id 
    AND sdcs.global_entity_id = sp.global_entity_id
  LEFT JOIN rdvr_dc_cmt AS rdrv
    ON TRIM(sdcs.country_name) = TRIM(rdrv.country_name)
    AND CAST(sdcs.month AS DATE) = CAST(rdrv.month AS DATE)
