-- This table extracts and maintains the customer-order-supplier mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 3
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_customer_order

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
),
tmp_sp_product AS (
  SELECT
    sp.global_entity_id,
    sp.country_code,
    sp.sku_id,
    COALESCE(CAST(sp.supplier_id AS STRING), '_unknown_') AS supplier_id,
    sp.supplier_name,
    sp.warehouse_id,
    sp.mapping_type,
    sp.sup_id_parent,
    sp.division_type,
    sp.global_supplier_id,
    COALESCE(sp.brand_name, '_unknown_') AS brand_name,
    COALESCE(sp.brand_owner_name, '_unknown_') AS brand_owner_name,
    COALESCE(sp.level_zero, '_unknown_') AS level_zero,
    COALESCE(sp.level_one, '_unknown_') AS level_one,
    COALESCE(sp.level_two, '_unknown_') AS level_two,
    COALESCE(sp.level_three, '_unknown_') AS level_three,
    sp.region_code,
    MAX(sp.updated_at) AS last_updated
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product` AS sp
  WHERE REGEXP_CONTAINS(sp.global_entity_id, {{ params.param_global_entity_id }})
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
),
ranked_global_product AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY global_entity_id, sku_id ORDER BY last_updated DESC) as recency_rank
  FROM tmp_sp_product
),
tmp_orders AS (
  SELECT
    DATE(o.order_created_date_lt) AS order_date,
    o.global_entity_id,
    o.country_code,
    o.currency_code,
        -- ORDER & CUSTOMER DETAILS
    o.platform_vendor_id AS vendor_id,
    o.analytical_customer_id,
    o.warehouse_id,
    o.order_id,
    o.order_status,
    o.payment_method,
    o.payment_provider,
    o.payment_type,
    o.invoice_number,
      -- ITEM & QUANTITY DETAILS
    i.sku AS sku_id,
    i.quantity_ordered AS ordered_quantity,
    i.quantity_returned AS returned_quantity,
    i.quantity_sold AS fulfilled_quantity,
    i.quantity_delivered AS delivered_quantity,
    i.pickup_issue,
    i.is_modified_quantity,
    i.is_modified_price,
        -- STATUS FLAGS
    o.is_successful AS is_sent,
    CASE WHEN i.quantity_returned > 0 THEN TRUE ELSE FALSE END AS is_returned,
    CASE
      WHEN NOT is_cancelled AND order_status = "sent" AND i.quantity_ordered <> i.quantity_delivered
        THEN TRUE
      ELSE FALSE
    END AS is_partial,
    --------------------------------------------------
    -- EURO PRICES
    --------------------------------------------------
    i.value_euro.total_amt_paid_eur AS total_price_paid_eur,
    i.value_euro.unit_price_listed_eur AS listed_unit_price_eur,
    i.value_euro.unit_price_paid_eur AS paid_unit_price_eur,
    i.value_euro.total_amt_paid_net_eur AS total_price_paid_net_eur,
    i.value_euro.amt_cogs_eur AS COGS_eur,
    i.value_euro.unit_discount_eur AS unit_discount_amount_eur,
    i.value_euro.unit_discount_eur AS unit_discount_eur,
    i.value_euro.unit_cost_eur AS unit_cost_eur,
    o.order_value_euro.total_amt_paid_net_eur AS amt_total_price_paid_net_eur,
    o.order_value_euro.amt_gbv_eur AS amt_gbv_eur,
    ROUND(CASE WHEN o.is_successful = TRUE THEN i.value_euro.unit_price_paid_eur ELSE 0 END - 
          CASE WHEN o.is_successful = TRUE THEN i.value_euro.unit_price_paid_eur * SAFE_DIVIDE(i.value_euro.total_amt_paid_net_eur, i.value_euro.total_amt_paid_eur) ELSE 0 END, 2) AS vat_unit_price_eur,
    SAFE_DIVIDE(COALESCE(i.value_euro.unit_discount_eur * i.quantity_sold, 0), 
              SAFE_DIVIDE(i.value_euro.total_amt_paid_eur,i.value_euro.total_amt_paid_net_eur)) as total_discount_eur,
    i.value_euro.unit_price_paid_eur * i.quantity_ordered AS total_amount_ordered_eur,
    i.value_euro.unit_cost_eur * i.quantity_ordered AS total_cogs_ordered_eur,
    i.value_euro.unit_price_paid_eur * i.quantity_delivered AS total_amount_delivered_eur,
    i.value_euro.unit_cost_eur * i.quantity_delivered AS total_cogs_delivered_eur,
    CASE WHEN o.is_successful = TRUE THEN i.quantity_sold * i.value_euro.unit_price_paid_eur ELSE 0 END AS total_amount_sold_eur,
    CASE WHEN o.is_successful = TRUE AND i.value_euro.unit_price_paid_eur > 0 THEN i.quantity_sold * i.value_euro.unit_price_paid_eur * SAFE_DIVIDE(i.value_euro.total_amt_paid_net_eur, i.value_euro.total_amt_paid_eur) ELSE 0 END AS total_amount_sold_net_eur,
    ROUND(CASE WHEN o.is_successful = TRUE THEN i.quantity_sold * i.value_euro.unit_price_paid_eur ELSE 0 END - 
          CASE WHEN o.is_successful = TRUE AND i.value_euro.unit_price_paid_eur > 0 THEN i.quantity_sold * i.value_euro.unit_price_paid_eur * SAFE_DIVIDE(i.value_euro.total_amt_paid_net_eur, i.value_euro.total_amt_paid_eur) ELSE 0 END, 2) AS vat_amount_eur,
    ROUND(CASE WHEN i.value_euro.total_amt_paid_net_eur = 0 THEN 0 ELSE (i.value_euro.total_amt_paid_eur - i.value_euro.total_amt_paid_net_eur) / i.value_euro.total_amt_paid_net_eur END, 2) AS vat_pct_eur,
    CASE WHEN i.value_euro.unit_discount_eur > 0 THEN i.value_euro.total_amt_paid_net_eur ELSE 0 END AS Net_Sales_from_promo_eur,
    COALESCE(i.value_euro.djini_order_items_supplier_funded_eur, 0) AS total_supplier_funding_eur,
    --------------------------------------------------
    -- LOCAL CURRENCY (LC) PRICES
    --------------------------------------------------
    i.value_lc.total_amt_paid_lc AS total_price_paid_lc,
    i.value_lc.unit_price_listed_lc AS listed_unit_price_lc,
    i.value_lc.unit_price_paid_lc AS paid_unit_price_lc,
    i.value_lc.total_amt_paid_net_lc AS total_price_paid_net_lc,
    i.value_lc.amt_cogs_lc AS COGS_lc,
    i.value_lc.unit_discount_lc AS unit_discount_amount_lc,
    i.value_lc.unit_discount_lc AS unit_discount_lc,
    i.value_lc.unit_cost_lc AS unit_cost_lc,
    o.order_value_lc.total_amt_paid_net_lc AS amt_total_price_paid_net_lc,
    o.order_value_lc.amt_gbv_lc AS amt_gbv_lc,
    ROUND(CASE WHEN o.is_successful = TRUE THEN i.value_lc.unit_price_paid_lc ELSE 0 END - 
          CASE WHEN o.is_successful = TRUE THEN i.value_lc.unit_price_paid_lc * SAFE_DIVIDE(i.value_lc.total_amt_paid_net_lc, i.value_lc.total_amt_paid_lc) ELSE 0 END, 2) AS vat_unit_price_lc,
    SAFE_DIVIDE(COALESCE(i.value_lc.unit_discount_lc * i.quantity_sold, 0), 
              SAFE_DIVIDE(i.value_lc.total_amt_paid_lc,i.value_lc.total_amt_paid_net_lc)) as total_discount_lc,
    i.value_lc.unit_price_paid_lc * i.quantity_ordered AS total_amount_ordered_lc,
    i.value_lc.unit_cost_lc * i.quantity_ordered AS total_cogs_ordered_lc,
    i.value_lc.unit_price_paid_lc * i.quantity_delivered AS total_amount_delivered_lc,
    i.value_lc.unit_cost_lc * i.quantity_delivered AS total_cogs_delivered_lc,
    CASE WHEN o.is_successful = TRUE THEN i.quantity_sold * i.value_lc.unit_price_paid_lc ELSE 0 END AS total_amount_sold_lc,
    CASE WHEN o.is_successful = TRUE AND i.value_lc.unit_price_paid_lc > 0 THEN i.quantity_sold * i.value_lc.unit_price_paid_lc * SAFE_DIVIDE(i.value_lc.total_amt_paid_net_lc, i.value_lc.total_amt_paid_lc) ELSE 0 END AS total_amount_sold_net_lc,
    ROUND(CASE WHEN o.is_successful = TRUE THEN i.quantity_sold * i.value_lc.unit_price_paid_lc ELSE 0 END - 
          CASE WHEN o.is_successful = TRUE AND i.value_lc.unit_price_paid_lc > 0 THEN i.quantity_sold * i.value_lc.unit_price_paid_lc * SAFE_DIVIDE(i.value_lc.total_amt_paid_net_lc, i.value_lc.total_amt_paid_lc) ELSE 0 END, 2) AS vat_amount_lc,
    ROUND(CASE WHEN i.value_lc.total_amt_paid_net_lc = 0 THEN 0 ELSE (i.value_lc.total_amt_paid_lc - i.value_lc.total_amt_paid_net_lc) / i.value_lc.total_amt_paid_net_lc END, 2) AS vat_pct_lc,
    CASE WHEN i.value_lc.unit_discount_lc > 0 THEN i.value_lc.total_amt_paid_net_lc ELSE 0 END AS Net_Sales_from_promo_lc,
    COALESCE(i.value_lc.djini_order_items_supplier_funded_lc, 0) AS total_supplier_funding_lc
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_orders` AS o
  LEFT JOIN UNNEST(o.items) AS i
  WHERE (DATE(o.order_created_date_lt) BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
    AND o.is_dmart IS TRUE
    AND o.is_successful IS TRUE
    AND REGEXP_CONTAINS(o.country_code, {{ params.param_country_code }})
)

SELECT 
  -- 1. Date & Geography
  te_o.order_date,
  te_o.global_entity_id,
  te_o.country_code,
  te_o.currency_code,
  -- 2. Order & Customer Details
  te_o.vendor_id,
  te_o.analytical_customer_id,
  te_o.warehouse_id,
  te_o.order_id,
  te_o.order_status,
  te_o.payment_method,
  te_o.payment_provider,
  te_o.payment_type,
  te_o.invoice_number,
  -- 3. Item & Quantity Details
  te_o.sku_id,
  te_o.ordered_quantity,
  te_o.returned_quantity,
  te_o.fulfilled_quantity,
  te_o.delivered_quantity,
  te_o.pickup_issue,
  te_o.is_modified_quantity,
  te_o.is_modified_price,
  -- 4. Status Flags
  te_o.is_sent,
  te_o.is_returned,
  te_o.is_partial,
  -- 5. Euro Metrics
  te_o.total_price_paid_eur,
  te_o.listed_unit_price_eur,
  te_o.paid_unit_price_eur,
  te_o.total_price_paid_net_eur,
  te_o.COGS_eur,
  te_o.unit_discount_amount_eur,
  te_o.unit_discount_eur,
  te_o.unit_cost_eur,
  te_o.amt_total_price_paid_net_eur,
  te_o.amt_gbv_eur,
  te_o.vat_unit_price_eur,
  te_o.total_discount_eur,
  te_o.total_amount_ordered_eur,
  te_o.total_cogs_ordered_eur,
  te_o.total_amount_delivered_eur,
  te_o.total_cogs_delivered_eur,
  te_o.total_amount_sold_eur,
  te_o.total_amount_sold_net_eur,
  te_o.vat_amount_eur,
  te_o.vat_pct_eur,
  te_o.Net_Sales_from_promo_eur,
  te_o.total_supplier_funding_eur,
  -- 6. Local Currency Metrics
  te_o.total_price_paid_lc,
  te_o.listed_unit_price_lc,
  te_o.paid_unit_price_lc,
  te_o.total_price_paid_net_lc,
  te_o.COGS_lc,
  te_o.unit_discount_amount_lc,
  te_o.unit_discount_lc,
  te_o.unit_cost_lc,
  te_o.amt_total_price_paid_net_lc,
  te_o.amt_gbv_lc,
  te_o.vat_unit_price_lc,
  te_o.total_discount_lc,
  te_o.total_amount_ordered_lc,
  te_o.total_cogs_ordered_lc,
  te_o.total_amount_delivered_lc,
  te_o.total_cogs_delivered_lc,
  te_o.total_amount_sold_lc,
  te_o.total_amount_sold_net_lc,
  te_o.vat_amount_lc,
  te_o.vat_pct_lc,
  te_o.Net_Sales_from_promo_lc,
  te_o.total_supplier_funding_lc,
  -- 7. Supplier & Product Metadata
  COALESCE(sp_exact.supplier_id, sp_fallback.supplier_id, '_unknown_') AS supplier_id,
  COALESCE(sp_exact.supplier_name, sp_fallback.supplier_name, '_unknown_') AS supplier_name,
  COALESCE(sp_exact.mapping_type, sp_fallback.mapping_type, 'qc_catalog') AS mapping_type,
  COALESCE(sp_exact.sup_id_parent, sp_fallback.sup_id_parent, '_unknown_') AS sup_id_parent,
  COALESCE(sp_exact.division_type, sp_fallback.division_type) AS division_type,
  CASE WHEN COALESCE(sp_exact.sup_id_parent, sp_fallback.sup_id_parent) = COALESCE(sp_exact.supplier_id, sp_fallback.supplier_id) THEN TRUE ELSE FALSE END AS is_parent_supplier,
  COALESCE(sp_exact.global_supplier_id, sp_fallback.global_supplier_id, '_unknown_') AS global_supplier_id,
  COALESCE(sp_exact.brand_name, sp_fallback.brand_name, '_unknown_') AS brand_name,
  COALESCE(sp_exact.brand_owner_name, sp_fallback.brand_owner_name, '_unknown_') AS brand_owner_name,
  COALESCE(sp_exact.level_zero, sp_fallback.level_zero, '_unknown_') AS level_zero,
  COALESCE(sp_exact.level_one, sp_fallback.level_one, '_unknown_') AS level_one,
  COALESCE(sp_exact.level_two, sp_fallback.level_two, '_unknown_') AS level_two,
  COALESCE(sp_exact.level_three, sp_fallback.level_three, '_unknown_') AS level_three,
  COALESCE(sp_exact.region_code, sp_fallback.region_code) AS region_code,
  -- 8. Partitioning
  CASE
    WHEN DATE_TRUNC(te_o.order_date, MONTH) = DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH) THEN CAST('{{ next_ds }}' AS DATE)
    ELSE LAST_DAY(DATE_TRUNC(te_o.order_date, MONTH))
  END AS partition_month
FROM tmp_orders AS te_o
LEFT JOIN tmp_sp_product AS sp_exact
  ON te_o.sku_id = sp_exact.sku_id
  AND te_o.global_entity_id = sp_exact.global_entity_id
  AND te_o.warehouse_id = sp_exact.warehouse_id
LEFT JOIN ranked_global_product AS sp_fallback
  ON te_o.sku_id = sp_fallback.sku_id
  AND te_o.global_entity_id = sp_fallback.global_entity_id
  AND sp_fallback.recency_rank = 1
