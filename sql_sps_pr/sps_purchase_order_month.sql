-- This table extracts and maintains the order metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 6.1
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_purchase_order_month
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
 , tmp_sp_product AS (
  SELECT
    sp.global_entity_id,
    sp.country_code,
    sp.sku_id,
    sp.warehouse_id,
    COALESCE(sp.brand_name, '_unknown_') AS brand_name, 
    COALESCE(sp.brand_owner_name, '_unknown_') AS brand_owner_name, 
    COALESCE(sp.level_one, '_unknown_') AS l1_master_category,
    COALESCE(sp.level_two, '_unknown_') AS l2_master_category,
    COALESCE(sp.level_three, '_unknown_') AS l3_master_category,
    ANY_VALUE(sp.sup_id_parent) AS principal_supplier_id,
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product` AS sp
  WHERE TRUE
    AND REGEXP_CONTAINS(sp.country_code, {{ params.param_country_code }})
  GROUP BY 1,2,3,4,5,6,7,8,9
),
tmp_supplier_performance_agg AS (
  SELECT
    spr.global_entity_id,
    spr.country_code,
    spr.sku_id,
    spr.warehouse_id,
    spr.supplier_id,
    spr.supplier_name,
    spr.po_order_id,
    spr.created_by,
    spr.order_received_on_time,
    spr.order_status,
    spr.cancel_reason,
    spr.po_canceled_on,
    CASE
      WHEN spr.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA')
           AND spr.created_by = 'Demand Planner'
        THEN TRUE 
      WHEN spr.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA')
        THEN TRUE 
      ELSE FALSE
      END AS is_compliant_flag,
    CAST(DATE_TRUNC(spr.create_date, MONTH) AS STRING) AS month,
    CAST(CONCAT('Q', EXTRACT(QUARTER FROM spr.create_date), '-', EXTRACT(YEAR FROM spr.create_date)) AS STRING) AS quarter_year,
    SUM(spr.total_received_qty_per_order) AS total_received_qty_per_order,
    SUM(spr.total_demanded_qty_per_order) AS total_demanded_qty_per_order,
  FROM `{{ params.project_id }}.{{ params.dataset.rl }}.supplier_performance_report` AS spr
  WHERE TRUE
    AND REGEXP_CONTAINS(spr.country_code, {{ params.param_country_code }})
    AND (spr.create_date BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
  GROUP BY ALL
)
  SELECT
    CASE
      WHEN DATE_TRUNC(CAST(spr_agg.month AS DATE), MONTH) = DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH)
      THEN CAST('{{ next_ds }}' AS DATE)
        ELSE LAST_DAY(CAST(spr_agg.month AS DATE))
    END AS partition_month,
    spr_agg.*,
    sp.* EXCEPT (country_code, sku_id, warehouse_id, global_entity_id)
  FROM tmp_supplier_performance_agg AS spr_agg
  LEFT JOIN tmp_sp_product AS sp
    ON sp.sku_id = spr_agg.sku_id
    AND sp.country_code = spr_agg.country_code
    AND sp.warehouse_id = spr_agg.warehouse_id
