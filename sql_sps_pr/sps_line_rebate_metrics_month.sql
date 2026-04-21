-- This table extracts and maintains the line rebate mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 4.1
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_line_rebate_metrics_month
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
  ,  sps_product AS (
      SELECT
        sp.global_entity_id,
        sp.sku_id,
        COALESCE(CAST(sp.supplier_id AS STRING), '_unknown_') AS supplier_id,
        -- CAST(sp.supplier_id AS STRING) AS supplier_id,
        sp.sup_id_parent AS principal_supplier_id,
        CASE WHEN CAST(sp.supplier_id AS STRING) = sp.sup_id_parent THEN TRUE END AS is_sup_id_parent,
        COALESCE( NULLIF(LOWER(sp.brand_name), 'unbranded'), '_unknown_' ) AS brand_name,
        COALESCE( NULLIF(LOWER(sp.brand_owner_name), 'unbranded'), '_unknown_' ) AS brand_owner_name,
        sp.global_supplier_id,
        COALESCE(sp.level_one, '_unknown_') AS l1_master_category,
        COALESCE(sp.level_two, '_unknown_') AS l2_master_category,
        COALESCE(sp.level_three, '_unknown_') AS l3_master_category,
      FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product` AS sp
      WHERE TRUE
        AND sp.country_code IN UNNEST(SPLIT({{ params.param_country_code }}, '|'))
      GROUP BY ALL
    ),
    sps_line_rebate AS (
      SELECT
        lr.global_entity_id,
        lr.country_code,
        lr.sku,
        lr.sup_id,
        CAST(DATE_TRUNC(lr.month, MONTH) AS STRING) AS month,
        CAST(CONCAT('Q', EXTRACT(QUARTER FROM lr.month), '-', EXTRACT(YEAR FROM lr.month)) AS STRING) AS quarter_year,
        ROUND(IFNULL(SUM(lr.calc_net_delivered), 0), 2) AS sku_calc_net_delivered,
        ROUND(IFNULL(SUM(lr.calc_net_return), 0), 2) AS sku_calc_net_return,
        ROUND(IFNULL(SUM(lr.rebate), 0), 2) AS sku_rebate,
        ROUND(IFNULL(SUM(CASE WHEN lr.trading_term_name != 'Distribution Allowance' THEN lr.rebate ELSE 0 END), 0), 4) AS sku_rebate_wo_dist_allowance_lc
      FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_line_rebate` AS lr
      WHERE TRUE
        AND lr.trading_term_type NOT IN ('Frontmargin')
        -- SOURCE FILTER ANCHORED TO QUARTER RANGES
        AND (DATE_TRUNC(DATE(lr.month), MONTH) BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
        AND lr.country_code IN UNNEST(SPLIT({{ params.param_country_code }}, '|'))
      GROUP BY 1, 2, 3, 4, 5, 6
    )
      SELECT
        lr.global_entity_id,
        lr.country_code,
        lr.sku,
        lr.sup_id AS supplier_id,
        lr.month,
        lr.quarter_year,
        lr.sku_calc_net_delivered,
        lr.sku_calc_net_return,
        lr.sku_rebate,
        lr.sku_rebate_wo_dist_allowance_lc,
        so.principal_supplier_id,
        so.brand_name,
        so.brand_owner_name,
        so.l1_master_category,
        so.l2_master_category,
        so.l3_master_category,
        CASE
          WHEN DATE_TRUNC(CAST(lr.month AS DATE), MONTH) = DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH)
          THEN CAST('{{ next_ds }}' AS DATE)
            ELSE LAST_DAY(CAST(lr.month AS DATE))
        END AS partition_month,
      FROM sps_line_rebate AS lr
      LEFT JOIN sps_product AS so
        ON lr.global_entity_id = so.global_entity_id
        AND lr.sku = so.sku_id
