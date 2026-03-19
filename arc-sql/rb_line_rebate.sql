/*STANDARD TERMS: Here we are calculating the amount per supplier*/
{%- if not params.backfill %}
  {% 
    set param_month = (
      data_interval_end.replace(day=1) - macros.dateutil.relativedelta.relativedelta(months=1)
    ).strftime('%Y-%m-%d') 
  %}
{%- elif params.is_backfill_chunks_enabled %}
  {% 
    set param_month = params.backfill_start_date
  %}
{%- endif %}

WITH valid_standard_terms_sup_amount_sku AS (
  SELECT vs.global_entity_id,
    vs.sup_id,
    vs.sku,
    vs.order_id,
    vs.received_local_time,
    vs.calc_gross_delivered,
    vs.calc_gross_return,
    vs.calc_net_delivered,
    vs.calc_net_return,
    SUM(vs.calc_gross_delivered) OVER (PARTITION BY vs.global_entity_id, vs.sup_id) AS sup_calc_gross_delivered,
    SUM(vs.calc_gross_return) OVER (PARTITION BY vs.global_entity_id, vs.sup_id) AS sup_calc_gross_return,
    SUM(vs.calc_net_delivered) OVER (PARTITION BY vs.global_entity_id, vs.sup_id) AS sup_calc_net_delivered,
    SUM(vs.calc_net_return) OVER (PARTITION BY vs.global_entity_id, vs.sup_id) AS sup_calc_net_return
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_valid_standard_terms` AS vs
  WHERE TRUE
  AND vs.month = '{{ param_month }}'
  AND REGEXP_CONTAINS(LOWER(vs.term_value_type), r'absolute') IS FALSE
  /*exclude order_type = original_of_last_updated_po - this orders are from previous month*/
  /*AND REGEXP_CONTAINS(LOWER(vs.order_type), r'original_of_last_updated_po') IS FALSE*/
  GROUP BY ALL
),
/*STANDARD TERMS: Here we are calculating the amount per supplier and term*/
valid_standard_terms_sup_term_amount_sku AS(
  SELECT vst.global_entity_id,
    vst.sup_id,
    vst.trading_term_name,
    vst.term_value_type,
    vst.term_applicability,
    vst.warehouse_type,
    vst.rebate_term_value,
    vst.sku,
    vst.order_id,
    vst.received_local_time,
    vst.calc_gross_delivered,
    vst.calc_gross_return,
    vst.calc_net_delivered,
    vst.calc_net_return,
    CASE
      WHEN REGEXP_CONTAINS(
    vst.global_entity_id,
        {{ params.distribution_param_global_entity_id }}
      )
      AND REGEXP_CONTAINS(LOWER(vst.term_value_type), r'percentage')
      AND REGEXP_CONTAINS(
        LOWER(vst.trading_term_name),
        r'distribution allowance'
      )
      AND REGEXP_CONTAINS(LOWER(vst.warehouse_type), r'dmart') THEN 0
      ELSE (
        CAST(
          IF(
            vst.gross_cost_without_vat IS NULL,
            0,
            vst.gross_cost_without_vat
          ) AS FLOAT64
        ) * CAST(
          IF(
            vst.delivered_quantity IS NULL,
            0,
            vst.delivered_quantity
          ) AS FLOAT64
        )
      )
    END AS term_sku_net_delivered,
    CASE
      WHEN REGEXP_CONTAINS(
        vst.global_entity_id,
        {{ params.distribution_param_global_entity_id }}
      )
      AND REGEXP_CONTAINS(LOWER(vst.term_value_type), r'percentage')
      AND REGEXP_CONTAINS(
        LOWER(vst.trading_term_name),
        r'distribution allowance'
      )
      AND REGEXP_CONTAINS(LOWER(vst.warehouse_type), r'dmart') THEN 0
      ELSE (
        CAST(
          IF(
            vst.gross_cost_without_vat IS NULL,
            0,
            vst.gross_cost_without_vat
          ) AS FLOAT64
        ) * CAST(
          IF(
            vst.returned_quantity IS NULL,
            0,
            vst.returned_quantity
          ) AS FLOAT64
        )
      )
    END AS term_sku_net_return,
    CASE
      WHEN REGEXP_CONTAINS(
        vst.global_entity_id,
        {{ params.distribution_param_global_entity_id }}
      )
      AND REGEXP_CONTAINS(LOWER(vst.term_value_type), r'percentage')
      AND REGEXP_CONTAINS(
        LOWER(vst.trading_term_name),
        r'distribution allowance'
      )
      AND REGEXP_CONTAINS(LOWER(vst.warehouse_type), r'dmart') THEN 0
      ELSE (
        CAST(
          IF(vst.gross_cost IS NULL, 0, vst.gross_cost) AS FLOAT64
        ) * CAST(
          IF(
            vst.delivered_quantity IS NULL,
            0,
            vst.delivered_quantity
          ) AS FLOAT64
        )
      )
    END AS term_sku_gross_delivered,
    CASE
      WHEN REGEXP_CONTAINS(
        vst.global_entity_id,
        {{ params.distribution_param_global_entity_id }}
      )
      AND REGEXP_CONTAINS(LOWER(vst.term_value_type), r'percentage')
      AND REGEXP_CONTAINS(
        LOWER(vst.trading_term_name),
        r'distribution allowance'
      )
      AND REGEXP_CONTAINS(LOWER(vst.warehouse_type), r'dmart') THEN 0
      ELSE (
        CAST(
          IF(vst.gross_cost IS NULL, 0, vst.gross_cost) AS FLOAT64
        ) * CAST(
          IF(
            vst.returned_quantity IS NULL,
            0,
            vst.returned_quantity
          ) AS FLOAT64
        )
      )
    END AS term_sku_gross_return,
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_valid_standard_terms` AS vst
    WHERE TRUE
    AND vst.month = '{{ param_month }}'
    AND REGEXP_CONTAINS(LOWER(vst.term_value_type), r'absolute') IS FALSE
  /*exclude order_type = original_of_last_updated_po - this orders are from previous month*/
      /*AND REGEXP_CONTAINS(LOWER(vst.order_type), r'original_of_last_updated_po') IS FALSE*/
  GROUP BY ALL
),
/*STANDAR TERMS: Here we are extracting the amount per supplier*/
valid_standard_terms_sup_amount AS (
  SELECT sak.global_entity_id,
    sak.sup_id,
    sak.sup_calc_gross_delivered,
    sak.sup_calc_gross_return,
    sak.sup_calc_net_delivered,
    sak.sup_calc_net_return,
    FROM valid_standard_terms_sup_amount_sku AS sak
  GROUP BY ALL
),
/*STANDARD TERMS: Here we are extracting the amount per supplier and term*/
valid_standard_terms_sup_term_amount AS (
  SELECT sak.global_entity_id,
    sak.sup_id,
    sak.trading_term_name,
    sak.term_value_type,
    sak.rebate_term_value,
    sak.term_applicability,
    SUM(sak.term_sku_gross_delivered) AS term_calc_gross_delivered,
    SUM(sak.term_sku_gross_return) AS term_calc_gross_return,
    SUM(sak.term_sku_net_delivered) AS term_calc_net_delivered,
    SUM(sak.term_sku_net_return) AS term_calc_net_return,
    FROM valid_standard_terms_sup_term_amount_sku AS sak
  GROUP BY ALL
),
/* !!!!!!FINAL STANDARD TERMS: FINAL SELECTION SCRIPT - Country, Contract, Term, Supplier, PO, SKU, Qty, Price, Valid Rebate*/
final_standard_terms AS(
  SELECT va.*
    EXCEPT (rebate, fixed_vat, vat_rate),
      ROUND(sup.sup_calc_gross_delivered,2) AS sup_valid_gross_purchase,
      ROUND(sup.sup_calc_gross_return,2) AS sup_valid_gross_return,
      ROUND(sup.sup_calc_net_delivered,2) AS sup_valid_net_purchase,
      ROUND(sup.sup_calc_net_return,2) AS sup_valid_net_return,
      ROUND(sup_term.term_calc_gross_delivered,2) AS sup_term_valid_gross_purchase,
      ROUND(sup_term.term_calc_gross_return,2) AS sup_term_valid_gross_return,
      ROUND(sup_term.term_calc_net_delivered,2) AS sup_term_valid_net_purchase,
      ROUND(sup_term.term_calc_net_return,2) AS sup_term_valid_net_return,
      va.rebate,
      SAFE_CAST(ROUND(va.vat_rate, 2) AS FLOAT64) AS fixed_vat_percentage,
      ROUND(COALESCE(va.gross_cost_without_vat, 0) * COALESCE(va.vat_rate, 0),4) AS fixed_vat,
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_valid_standard_terms` AS va
    LEFT JOIN valid_standard_terms_sup_amount AS sup ON va.global_entity_id = sup.global_entity_id
    AND va.sup_id = sup.sup_id
    LEFT JOIN valid_standard_terms_sup_term_amount AS sup_term ON va.global_entity_id = sup_term.global_entity_id
    AND va.sup_id = sup_term.sup_id
    AND va.trading_term_name = sup_term.trading_term_name
    AND va.rebate_term_value = sup_term.rebate_term_value
    AND va.term_value_type = sup_term.term_value_type
    AND va.term_applicability = sup_term.term_applicability
  WHERE TRUE
    AND va.month = '{{ param_month }}'
    AND va.rebate <> 0
    /* only not zero rebates - to reduce the number of rows*/
    GROUP BY ALL
)
/*FINAL SELECT*/
SELECT 'standard' AS term_bucket,
  current_datetime() AS insert_date,
  DATE('{{ param_month }}') AS month,
  global_entity_id,
  country_code,
  country,
  global_supplier_id,
  supplier_name,
  finance_system_id,
  sup_id,
  unit_of_measure,
  trading_term_name,
  trading_term_type,
  rebate_term_value,
  term_value_type,
  term_start_date,
  term_end_date,
  calculated_term_start_date,
  calculated_term_end_date,
  term_applicability,
  term_frequency,
  rebate_currency,
  term_currency,
  contract_id,
  opp_id,
  account_id,
  account_owner,
  contract_startdate,
  contract_enddate,
  contract_effective_enddate,
  contract_status,
  contract_term_type,
  contract_term_id,
  opp_term_id,
  source_table,
  warehouse_type,
  term_brand_name,
  term_pim_brand_id,
  term_brand_category_name,
  order_id,
  po_id,
  received_local_time,
  sku,
  sku_brand_name,
  sku_categ_level_one,
  sku_categ_level_two,
  sku_level_three,
  sku_level_four,
  sku_level_five,
  sku_level_six,
  gross_cost_without_vat,
  gross_cost,
  delivered_quantity,
  returned_quantity,
  trade_cost,
  fixed_discount,
  extra_discount,
  fixed_vat,
  fixed_vat_percentage,
  extra_vat,
  internal_tax,
  order_type,
  tier_term_rebate_type,
  tier_term_rebate,
  tier_term_threshold,
  tier_term_number,
  calc_gross_delivered,
  calc_gross_return,
  calc_net_delivered,
  calc_net_return,
  sup_valid_gross_purchase,
  sup_valid_gross_return,
  sup_valid_net_purchase,
  sup_valid_net_return,
  sup_term_valid_gross_purchase,
  sup_term_valid_gross_return,
  sup_term_valid_net_purchase,
  sup_term_valid_net_return,
  rebate,
  warehouse_id,
  barcodes,
  CAST(fixing_condition AS STRING) AS fixing_condition,
  reason,
  term_comments,
  sku_name,
  ordered_quantity,
  warehouse_name,
  excise_tax,
FROM final_standard_terms
