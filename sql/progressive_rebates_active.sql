--The purpose of this table is to calculate progressive_terms at the supplier and trading term level for each country
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

WITH date_params AS (
  SELECT
    DATE('{{ param_month }}') AS param_month,
    DATE_TRUNC('{{ param_month }}', MONTH) AS current_month,
    DATE_SUB(DATE_TRUNC('{{ param_month }}', MONTH), INTERVAL 1 MONTH) AS prev_month,
    DATE_TRUNC(DATE_SUB('{{ param_month }}', INTERVAL 1 YEAR), MONTH) AS prev_year_month_start,
    DATE_TRUNC('{{ param_month }}', QUARTER) AS current_quarter_start,
    DATE_SUB(DATE_TRUNC('{{ param_month }}', QUARTER), INTERVAL 1 QUARTER) AS prev_quarter_start,
    DATE_TRUNC(DATE_SUB('{{ param_month }}', INTERVAL 1 YEAR), QUARTER) AS prev_year_quarter_start,
    DATE_SUB(DATE_TRUNC(DATE_ADD('{{ param_month }}', INTERVAL 1 QUARTER), QUARTER), INTERVAL 1 DAY) AS current_quarter_end,
    DATE_TRUNC('{{ param_month }}', YEAR) AS current_year_start,
    DATE_TRUNC(DATE_SUB('{{ param_month }}', INTERVAL 1 YEAR), YEAR) AS prev_year_year_start,
    DATE_SUB(DATE_TRUNC(DATE_ADD('{{ param_month }}', INTERVAL 1 YEAR), YEAR), INTERVAL 1 DAY) AS current_year_end,
    EXTRACT(MONTH FROM DATE('{{ param_month }}')) IN (3, 6, 9, 12) AS is_end_of_quarter,
    EXTRACT(MONTH FROM DATE('{{ param_month }}')) = 12 AS is_end_of_year
),
param_supplier_mapping_cte AS (
    SELECT
        psm.*
    FROM `{{ params.project_id }}.{{ params.dataset.dl }}.gsheet_gsh_srm_principal_division_allocation`  AS psm
    GROUP BY ALL
),
currency AS (
  SELECT
    cr.currency_iso_code AS rebate_currency,
    LOWER(cr.country_code) AS country_code
  FROM `fulfillment-dwh-production.curated_data_shared_coredata.global_entities` AS cr
  GROUP BY ALL
),
products AS (
  SELECT
    p.sku,
    p.sku_name,
    p.barcodes,
    p.country_code,
    p.brand_name,
    p.categ_level_one,
    p.categ_level_two,
    p.categ_level_three,
    p.categ_level_four,
    p.categ_level_five,
    p.categ_level_six,
    p.master_created_at AS sku_created_at,
    p.pim_product_id,
    p.pim_brand_id,
    p.pim_category_id
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_products` AS p
    WHERE TRUE
  GROUP BY ALL
),
contract_base AS (
  SELECT
    co.contract_term_id,
    co.sup_id,
    CAST(ps.principal_supplier_id AS STRING) AS principal_supplier_id_mapped,
    COALESCE(CAST(ps.principal_supplier_id AS STRING),co.sup_id) AS sup_id_mapped,
    co.country_code,
    co.contract_status,
    co.term_frequency,
    co.term_start_date,
    co.term_end_date,
    co.contract_enddate,
    co.contract_effective_enddate,
    co.calculated_against,
    co.brand_category_pim,
    co.trading_term_cluster,
    co.term_applicability,
    co.valid_terms,
    co.term_brand_category_name,
    co.tier_term_rebate_type,
    co.tier_term_rebate,
    co.tier_term_threshold,
    co.tier_thresholdtype,
    co.tier_term_number,
    co.month
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_contract_info`AS co
  LEFT JOIN param_supplier_mapping_cte AS ps
    ON co.country_code = ps.country_code
    AND co.sup_id = CAST(ps.division_supplier_id AS STRING)
  WHERE TRUE
    AND co.month = '{{ param_month }}'
  AND REGEXP_CONTAINS(co.country_code, {{ params.param_country_code }})
  GROUP BY ALL
),
adjusted_contract_dates AS (
  SELECT
    co.contract_term_id,
    co.contract_status,
    co.term_frequency,
    co.term_start_date,
    co.term_end_date,
    co.contract_enddate,
    co.contract_effective_enddate,
    CASE
      WHEN REGEXP_CONTAINS(co.term_frequency, r'Monthly') THEN CAST(DATE_TRUNC(dp.param_month, MONTH) AS DATETIME)
      WHEN REGEXP_CONTAINS(co.term_frequency, r'Quarterly') THEN CAST(DATE_TRUNC(dp.param_month, QUARTER) AS DATETIME)
      WHEN REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time') THEN CAST(DATE_TRUNC(dp.param_month, YEAR) AS DATETIME)
      ELSE NULL
    END AS ideal_report_period_start_datetime,
    CASE
      WHEN REGEXP_CONTAINS(co.term_frequency, r'Monthly') THEN CAST(DATE_ADD(LAST_DAY(dp.param_month, MONTH), INTERVAL 1 DAY) AS DATETIME)
      WHEN REGEXP_CONTAINS(co.term_frequency, r'Quarterly') THEN CAST(DATE_ADD(LAST_DAY(dp.param_month, QUARTER), INTERVAL 1 DAY) AS DATETIME)
      WHEN REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time') THEN CAST(DATE_ADD(LAST_DAY(dp.param_month, YEAR), INTERVAL 1 DAY) AS DATETIME)
      ELSE NULL
    END AS ideal_report_period_end_exclusive_datetime,
    CASE
      WHEN REGEXP_CONTAINS(co.contract_status, r'Active - Extended') AND DATE_TRUNC(co.term_start_date, MONTH) < DATE_SUB(DATE_TRUNC(dp.param_month, MONTH), INTERVAL 36 MONTH) THEN DATE_ADD(co.term_start_date, INTERVAL 3 YEAR)
      WHEN REGEXP_CONTAINS(co.contract_status, r'Active - Extended') AND DATE_TRUNC(co.term_start_date, MONTH) < DATE_SUB(DATE_TRUNC(dp.param_month, MONTH), INTERVAL 24 MONTH) THEN DATE_ADD(co.term_start_date, INTERVAL 2 YEAR)
      WHEN REGEXP_CONTAINS(co.contract_status, r'Active - Extended') AND DATE_TRUNC(co.term_start_date, MONTH) < DATE_SUB(DATE_TRUNC(dp.param_month, MONTH), INTERVAL 12 MONTH) THEN DATE_ADD(co.term_start_date, INTERVAL 1 YEAR)
      ELSE co.term_start_date
    END AS effective_contract_start_date,
  CASE
      WHEN REGEXP_CONTAINS(co.contract_status, r'Active - Extended|Active') AND REGEXP_CONTAINS(co.term_frequency, r'Quarterly') THEN DATE_TRUNC(LAST_DAY(DATE_TRUNC(dp.param_month, QUARTER), QUARTER), MONTH)
      WHEN REGEXP_CONTAINS(co.contract_status, r'Active - Extended|Active') AND REGEXP_CONTAINS(co.term_frequency, r'Monthly') THEN DATE_TRUNC(LAST_DAY(DATE_TRUNC(dp.param_month, MONTH), MONTH), MONTH)
      WHEN REGEXP_CONTAINS(co.contract_status, r'Active') AND REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time') THEN DATE_TRUNC(LAST_DAY(DATE_TRUNC(dp.param_month, YEAR), YEAR), MONTH)
      WHEN REGEXP_CONTAINS(co.contract_status, r'Active - Extended') AND REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time') THEN DATE_TRUNC(LAST_DAY(DATE_TRUNC(dp.param_month, YEAR), YEAR), MONTH)
      WHEN co.contract_status = 'Active' THEN DATE_TRUNC(co.contract_enddate, MONTH)
      ELSE DATE(dp.param_month)
    END AS effective_contract_end_date,
    co.calculated_against,
    co.brand_category_pim
  FROM contract_base AS co
  CROSS JOIN date_params AS dp
  GROUP BY ALL
),
monthly_sku_calculations AS (
  SELECT
    oss.country_code,
    oss.sup_id,
    CAST(ps.principal_supplier_id AS STRING) AS principal_supplier_id_mapped,
    COALESCE(CAST(ps.principal_supplier_id AS STRING),oss.sup_id) AS sup_id_mapped,
    oss.sku,
    p.sku_name,
    p.brand_name,
    p.categ_level_one,
    p.categ_level_two,
    p.categ_level_three,
    p.categ_level_four,
    p.categ_level_five,
    p.categ_level_six,
    p.barcodes,
    p.pim_product_id,
    p.pim_brand_id,
    p.pim_category_id,
    p.sku_created_at,
    DATE_TRUNC(oss.received_local_time, MONTH) AS received_local_month,
    IFNULL(SUM(CAST(oss.gross_cost_without_vat AS FLOAT64) * CAST(oss.delivered_quantity AS INT64)), 0) - IFNULL(SUM(CAST(oss.gross_cost_without_vat AS FLOAT64) * CAST(oss.returned_quantity AS INT64)), 0) AS net_amount,
    IFNULL(SUM(CAST(oss.gross_cost AS FLOAT64) * CAST(oss.delivered_quantity AS INT64)), 0) - IFNULL(SUM(CAST(oss.gross_cost AS FLOAT64) * CAST(oss.returned_quantity AS INT64)), 0) AS gross_amount
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_orderline_sku` AS oss
  LEFT JOIN param_supplier_mapping_cte AS ps
    ON oss.country_code = ps.country_code
    AND oss.sup_id = CAST(ps.division_supplier_id AS STRING)
  LEFT JOIN products AS p
    ON oss.sku = p.sku AND oss.country_code = p.country_code
  CROSS JOIN date_params AS dp
  WHERE TRUE
    AND oss.month = '{{ param_month }}'
    AND DATE_TRUNC(oss.received_local_time, MONTH) >= DATE_TRUNC(DATE_SUB(DATE(dp.param_month), INTERVAL 1 YEAR), YEAR)
  GROUP BY ALL
),
last_two_years AS ( 
  --basically multiplies purchases per sku lines by times of contract terms that there is per supplier and by times of tier numbers there is per ontract_term
  SELECT
    dp.param_month,
    dp.current_month, 
    dp.prev_month, 
    dp.prev_year_month_start, 
    dp.current_quarter_start, 
    dp.prev_quarter_start, 
    dp.prev_year_quarter_start, 
    dp.current_quarter_end, 
    dp.current_year_start, 
    dp.prev_year_year_start, 
    dp.current_year_end, 
    dp.is_end_of_quarter, 
    dp.is_end_of_year, 
    md.received_local_month,
    md.country_code,
    md.sup_id,
    md.principal_supplier_id_mapped,
    md.sup_id_mapped,
    md.sku,
    md.sku_name,
    md.brand_name,
    md.categ_level_one,
    md.categ_level_two,
    md.categ_level_three,
    md.categ_level_four,
    md.categ_level_five,
    md.categ_level_six,
    md.pim_product_id,
    md.pim_brand_id,
    md.pim_category_id,
    md.net_amount,
    md.sku_created_at,
    co.contract_term_id,
    co.contract_status,
    co.term_frequency,
    co.term_applicability,
    co.calculated_against,
    co.valid_terms,
    co.term_brand_category_name,
    co.brand_category_pim,
    co.tier_term_rebate_type,
    co.tier_term_rebate,
    co.tier_term_threshold,
    co.tier_thresholdtype,
    co.tier_term_number,
    co.term_start_date,
    co.term_end_date,
    co.contract_enddate,
    co.contract_effective_enddate,
    co.trading_term_cluster,
    cr.rebate_currency,
    acd.effective_contract_start_date,
    acd.effective_contract_end_date,
    acd.ideal_report_period_start_datetime,
    acd.ideal_report_period_end_exclusive_datetime
  FROM monthly_sku_calculations AS md
  CROSS JOIN date_params AS dp
  JOIN contract_base AS co
    ON (co.sup_id = md.sup_id_mapped OR co.sup_id = md.sup_id)
    AND co.country_code = md.country_code
    AND co.trading_term_cluster = 'progressive_terms'
    AND (co.contract_status = 'Active' OR co.contract_status = 'Active - Extended')
  LEFT JOIN currency AS cr
    ON co.country_code = cr.country_code
  LEFT JOIN adjusted_contract_dates AS acd
    ON co.contract_term_id = acd.contract_term_id
  WHERE md.received_local_month BETWEEN dp.prev_year_year_start AND dp.param_month
),
 sku_aggregated_data AS (
  SELECT
    lty.* EXCEPT(param_month, current_month, prev_month, prev_year_month_start, current_quarter_start, prev_quarter_start, prev_year_quarter_start, current_quarter_end, current_year_start, prev_year_year_start, current_year_end, is_end_of_quarter, is_end_of_year),
    lty.param_month, lty.current_month, lty.prev_month, lty.prev_year_month_start, lty.current_quarter_start, lty.prev_quarter_start, lty.prev_year_quarter_start, lty.current_quarter_end, lty.current_year_start, lty.prev_year_year_start, lty.current_year_end, lty.is_end_of_quarter, lty.is_end_of_year,
    
    -- Original SKU validation flag
    CASE
      WHEN lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id) THEN '2_valid_brand'
      WHEN lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) THEN '3_valid_categ'
      WHEN REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product') THEN '1_valid_sup'
      WHEN REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku) THEN '4_valid_sku'
      WHEN REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND (
        COALESCE(LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id), TRUE) OR
        COALESCE(LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id), TRUE) OR
        LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)
      ) THEN '5_valid_other_skus'
      ELSE 'not_valid'
    END AS valid_sku_term,
    
    -- APPLYING THE FILTER LOGIC TO ALL WINDOW SUMS:
    SUM(CASE WHEN lty.received_local_month = lty.current_month AND (
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product')) OR
        (lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
        (lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id))) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND NOT (
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) OR
            (LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku))
        ))
    ) THEN lty.net_amount ELSE 0 END) OVER (PARTITION BY lty.country_code, lty.sup_id_mapped, lty.contract_term_id, CAST(lty.tier_term_number AS STRING)) AS sum_net_amount_current_month_sup_term,
    
    SUM(CASE WHEN lty.received_local_month = lty.prev_month AND (
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product')) OR
        (lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
        (lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id))) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND NOT (
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) OR
            (LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku))
        ))
    ) THEN lty.net_amount ELSE 0 END) OVER (PARTITION BY lty.country_code, lty.sup_id_mapped, lty.contract_term_id, CAST(lty.tier_term_number AS STRING)) AS sum_net_amount_prev_month_sup_term,
    
    SUM(CASE WHEN lty.received_local_month = lty.prev_year_month_start AND (
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product')) OR
        (lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
        (lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id))) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND NOT (
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) OR
            (LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku))
        ))
    ) THEN lty.net_amount ELSE 0 END) OVER (PARTITION BY lty.country_code, lty.sup_id_mapped, lty.contract_term_id, CAST(lty.tier_term_number AS STRING)) AS sum_net_amount_prev_year_month_sup_term,
    
    SUM(CASE WHEN lty.is_end_of_quarter AND lty.received_local_month BETWEEN lty.current_quarter_start AND lty.current_quarter_end AND (
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product')) OR
        (lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
        (lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id))) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND NOT (
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) OR
            (LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku))
        ))
    ) THEN lty.net_amount ELSE 0 END) OVER (PARTITION BY lty.country_code, lty.sup_id_mapped, lty.contract_term_id, CAST(lty.tier_term_number AS STRING)) AS sum_net_amount_current_quarter_sup_term,
    
    SUM(CASE WHEN lty.is_end_of_quarter AND lty.received_local_month BETWEEN lty.prev_quarter_start AND lty.current_quarter_start AND (
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product')) OR
        (lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
        (lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id))) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND NOT (
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) OR
            (LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku))
        ))
    ) THEN lty.net_amount ELSE 0 END) OVER (PARTITION BY lty.country_code, lty.sup_id_mapped, lty.contract_term_id, CAST(lty.tier_term_number AS STRING)) AS sum_net_amount_prev_quarter_sup_term,
    
    SUM(CASE WHEN lty.is_end_of_quarter AND lty.received_local_month BETWEEN lty.prev_year_quarter_start AND DATE_TRUNC(DATE_SUB(lty.current_quarter_end, INTERVAL 1 YEAR), MONTH) AND (
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product')) OR
        (lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
        (lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id))) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND NOT (
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) OR
            (LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku))
        ))
    ) THEN lty.net_amount ELSE 0 END) OVER (PARTITION BY lty.country_code, lty.sup_id_mapped, lty.contract_term_id, CAST(lty.tier_term_number AS STRING)) AS sum_net_amount_prev_year_quarter_sup_term,
    
    SUM(CASE WHEN lty.received_local_month BETWEEN lty.current_year_start AND lty.current_month AND (
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product')) OR
        (lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
        (lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id))) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND NOT (
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) OR
            (LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku))
        ))
    ) THEN lty.net_amount ELSE 0 END) OVER (PARTITION BY lty.country_code, lty.sup_id_mapped, lty.contract_term_id, CAST(lty.tier_term_number AS STRING)) AS sum_net_amount_current_year_sup_term,
    
    SUM(CASE WHEN lty.received_local_month BETWEEN lty.prev_year_year_start AND DATE_TRUNC(DATE_SUB(lty.current_month, INTERVAL 1 YEAR), MONTH) AND (
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all master product')) OR
        (lty.term_applicability = 'Brand' AND LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
        (lty.term_applicability = 'Category' AND (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id))) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'specific master product') AND LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku)) OR
        (REGEXP_CONTAINS(LOWER(lty.term_applicability), r'all other master product') AND NOT (
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_brand_id)) OR
            (LOWER(lty.brand_category_pim) = LOWER(lty.pim_category_id)) OR
            (LOWER(lty.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(lty.brand_category_pim), ',')) AS sku))
        ))
    ) THEN lty.net_amount ELSE 0 END) OVER (PARTITION BY lty.country_code, lty.sup_id_mapped, lty.contract_term_id, CAST(lty.tier_term_number AS STRING)) AS sum_net_amount_prev_year_sup_term
  FROM last_two_years AS lty
  GROUP BY ALL
),
sku_sup_aggregated_data AS (
  SELECT DISTINCT
    ad.country_code,
    ad.sup_id,
    ad.principal_supplier_id_mapped,
    ad.sup_id_mapped,
    ad.contract_term_id,
    ad.contract_status,
    ad.term_frequency,
    ad.term_applicability,
    ad.calculated_against,
    ad.valid_terms,
    ad.valid_sku_term,
    ad.term_brand_category_name,
    ad.tier_term_rebate_type,
    ad.tier_term_rebate,
    ad.tier_term_threshold,
    ad.tier_thresholdtype,
    ad.tier_term_number,
    ad.rebate_currency,
    
    -- Date/Contract Fields (Using MIN on sku_created_at for grouping)
    MIN(ad.sku_created_at) AS sku_created_at, 
    ad.term_start_date,
    ad.term_end_date,
    ad.contract_enddate,
    ad.contract_effective_enddate,
    ad.trading_term_cluster,
    GREATEST(ad.effective_contract_start_date, DATE(ad.ideal_report_period_start_datetime)) AS calculated_term_start_date,
    DATE(LEAST(ad.effective_contract_end_date, DATE(ad.ideal_report_period_end_exclusive_datetime))) AS calculated_term_end_date,
    ad.prev_year_month_start,

    -- Calculated Net Amounts (These are already aggregated by window functions)
    ad.sum_net_amount_prev_month_sup_term,
    ad.sum_net_amount_current_month_sup_term,
    ad.sum_net_amount_prev_year_month_sup_term,
    ad.sum_net_amount_prev_quarter_sup_term,
    ad.sum_net_amount_current_quarter_sup_term,
    ad.sum_net_amount_prev_year_quarter_sup_term,
    ad.sum_net_amount_current_year_sup_term,
    ad.sum_net_amount_prev_year_sup_term,
    
    -- Deviation Metrics
    CASE
      WHEN MIN(ad.sku_created_at) IS NULL OR DATE(MIN(ad.sku_created_at)) > ad.prev_year_month_start OR ad.sum_net_amount_prev_month_sup_term <= 0 THEN 0
      ELSE ROUND((SAFE_DIVIDE(ad.sum_net_amount_current_month_sup_term, ad.sum_net_amount_prev_month_sup_term) - 1), 2)
    END AS deviation_month_vs_prev_month_sup_term,
    CASE
      WHEN MIN(ad.sku_created_at) IS NULL OR DATE(MIN(ad.sku_created_at)) > ad.prev_year_month_start OR ad.sum_net_amount_prev_year_month_sup_term <= 0 THEN 0
      ELSE ROUND((SAFE_DIVIDE(ad.sum_net_amount_current_month_sup_term, ad.sum_net_amount_prev_year_month_sup_term) - 1), 2)
    END AS deviation_month_vs_prev_year_month_sup_term,
    CASE
      WHEN MIN(ad.sku_created_at) IS NULL OR DATE(MIN(ad.sku_created_at)) > ad.prev_year_month_start OR ad.sum_net_amount_prev_quarter_sup_term <= 0 THEN 0
      ELSE ROUND((SAFE_DIVIDE(ad.sum_net_amount_current_quarter_sup_term, ad.sum_net_amount_prev_quarter_sup_term) - 1), 2)
    END AS deviation_quarter_vs_prev_quarter,
    CASE
      WHEN MIN(ad.sku_created_at) IS NULL OR DATE(MIN(ad.sku_created_at)) > ad.prev_year_month_start OR ad.sum_net_amount_prev_year_quarter_sup_term <= 0 THEN 0
      ELSE ROUND((SAFE_DIVIDE(ad.sum_net_amount_current_quarter_sup_term, ad.sum_net_amount_prev_year_quarter_sup_term) - 1), 2)
    END AS deviation_quarter_vs_prev_year,
    CASE
      WHEN MIN(ad.sku_created_at) IS NULL OR DATE(MIN(ad.sku_created_at)) > ad.prev_year_month_start OR ad.sum_net_amount_prev_year_sup_term <= 0 THEN 0
      ELSE ROUND((SAFE_DIVIDE(ad.sum_net_amount_current_year_sup_term, ad.sum_net_amount_prev_year_sup_term) - 1), 2)
    END AS deviation_year_vs_prev_year,

    -- Diff vs Target Calculation
    CASE
    --ABSOLUTE
    -- We asume if its absolute then it should be compared against anything else than the threshold term itself
      WHEN ad.tier_thresholdtype = 'Absolute' THEN 
        CASE WHEN ad.term_frequency = 'Monthly' THEN ad.sum_net_amount_current_month_sup_term - ad.tier_term_threshold
             WHEN ad.term_frequency = 'Quarterly' THEN ad.sum_net_amount_current_quarter_sup_term - ad.tier_term_threshold
             WHEN ad.term_frequency IN ('One time', 'Annually') THEN ad.sum_net_amount_current_year_sup_term - ad.tier_term_threshold END
      --PERCENTAGE
      -- We asume that if the calculated_against field in SRM is not filled then BY DEFAULT is year over year growth vs the term itself 
      WHEN ad.tier_thresholdtype = 'Percentage' AND ad.calculated_against != 'Previous Period' THEN
        CASE WHEN ad.term_frequency = 'Monthly' THEN (SAFE_DIVIDE(ad.sum_net_amount_current_month_sup_term , ad.sum_net_amount_prev_year_month_sup_term) - 1) -SAFE_DIVIDE(ad.tier_term_threshold,100)
             WHEN ad.term_frequency = 'Quarterly' THEN (SAFE_DIVIDE(ad.sum_net_amount_current_quarter_sup_term , ad.sum_net_amount_prev_year_quarter_sup_term)-1) -SAFE_DIVIDE(ad.tier_term_threshold,100)
             WHEN ad.term_frequency IN ('One time', 'Annually') THEN (SAFE_DIVIDE(ad.sum_net_amount_current_year_sup_term , ad.sum_net_amount_prev_year_sup_term)-1)-SAFE_DIVIDE(ad.tier_term_threshold,100) END
      --VS CORRESPONDING LAST PERIOD i.e. Q3 vs Q2, M10 vs M9, Y2 vs Y1
      WHEN ad.tier_thresholdtype = 'Percentage' AND ad.calculated_against = 'Previous Period' THEN
        CASE WHEN ad.term_frequency = 'Monthly' THEN (SAFE_DIVIDE(ad.sum_net_amount_current_month_sup_term, ad.sum_net_amount_prev_month_sup_term) - 1) -SAFE_DIVIDE(ad.tier_term_threshold,100)
             WHEN ad.term_frequency = 'Quarterly' THEN (SAFE_DIVIDE(ad.sum_net_amount_current_quarter_sup_term, ad.sum_net_amount_prev_quarter_sup_term) - 1) -SAFE_DIVIDE(ad.tier_term_threshold,100)
             WHEN ad.term_frequency IN ('One time', 'Annually') THEN (SAFE_DIVIDE(ad.sum_net_amount_current_year_sup_term, ad.sum_net_amount_prev_year_sup_term)- 1) -SAFE_DIVIDE(ad.tier_term_threshold,100) END
    END AS diff_vs_target,
  FROM sku_aggregated_data AS ad
  WHERE ad.valid_sku_term <> 'not_valid'
    AND REGEXP_CONTAINS(ad.country_code, {{ params.param_country_code }})
  GROUP BY ALL
),
sup_aggregated_data AS (
  SELECT
    suad.* EXCEPT( sku_created_at, valid_sku_term, prev_year_month_start, calculated_term_start_date, calculated_term_end_date, deviation_month_vs_prev_month_sup_term, deviation_month_vs_prev_year_month_sup_term, deviation_quarter_vs_prev_quarter, deviation_quarter_vs_prev_year, deviation_year_vs_prev_year),
    suad.sku_created_at, 
    suad.valid_sku_term, 
    suad.prev_year_month_start,
    suad.calculated_term_start_date,
    suad.calculated_term_end_date,
    suad.deviation_month_vs_prev_month_sup_term,
    suad.deviation_month_vs_prev_year_month_sup_term,
    suad.deviation_quarter_vs_prev_quarter,
    suad.deviation_quarter_vs_prev_year,
    suad.deviation_year_vs_prev_year,
    ROW_NUMBER() OVER (
      PARTITION BY suad.country_code, suad.sup_id_mapped, suad.contract_term_id
      ORDER BY
        CASE WHEN suad.diff_vs_target IS NOT NULL AND suad.diff_vs_target >= 0 THEN 1
          ELSE 0
        END DESC,
        suad.tier_term_threshold DESC,
        SAFE_CAST(suad.tier_term_rebate AS INT64) DESC
    ) AS rn
  FROM sku_sup_aggregated_data AS suad
  GROUP BY ALL
),
suppliers AS (
  SELECT
    ps.country_code,
    CAST(s.supplier_id AS STRING) AS sup_id,
    CAST(s.supplier_finance_id AS STRING) AS supplier_finance_id,
    CAST(s.supplier_name AS STRING) AS supplier_name
FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared }}.products_suppliers` AS ps
  LEFT JOIN UNNEST(ps.suppliers) AS s
  LEFT JOIN UNNEST(s.warehouses) AS w
  WHERE TRUE
    -- AND s.is_preferred IS TRUE
    AND REGEXP_CONTAINS(ps.country_code, {{ params.param_country_code }})
  GROUP BY ALL
)
SELECT
  -- Final Output Columns, aligned to the Term-Tier level and format
  supa.country_code,
  supa.sup_id,
  supa.principal_supplier_id_mapped,
  supa.sup_id_mapped,
  sp.supplier_name,
  sp.supplier_finance_id,
  supa.contract_term_id,
  supa.contract_status,
  supa.term_frequency,
  supa.calculated_against,
  supa.trading_term_cluster,
  supa.rebate_currency,
  supa.term_applicability,
  supa.valid_terms,
  supa.valid_sku_term,
  supa.term_brand_category_name,
  supa.tier_term_rebate_type,
  supa.tier_term_rebate,
  supa.tier_term_threshold,
  supa.tier_thresholdtype,
  supa.tier_term_number,
  
  -- Date Fields
  supa.term_start_date,
  supa.term_end_date,
  supa.contract_enddate,
  supa.contract_effective_enddate,
  supa.calculated_term_start_date,
  supa.calculated_term_end_date,
  -- supa.sku_created_at, -- EXCLUDING 
  supa.prev_year_month_start,

  -- Calculated Net Amounts
  supa.sum_net_amount_current_month_sup_term,
  supa.sum_net_amount_prev_month_sup_term,
  supa.sum_net_amount_prev_year_month_sup_term,
  supa.sum_net_amount_current_quarter_sup_term,
  supa.sum_net_amount_prev_quarter_sup_term,
  supa.sum_net_amount_prev_year_quarter_sup_term,
  supa.sum_net_amount_current_year_sup_term,
  supa.sum_net_amount_prev_year_sup_term,
  
  -- Deviation Metrics
  supa.deviation_month_vs_prev_month_sup_term,
  supa.deviation_month_vs_prev_year_month_sup_term,
  supa.deviation_quarter_vs_prev_quarter,
  supa.deviation_quarter_vs_prev_year,
  supa.deviation_year_vs_prev_year,

  -- Final Flags and Values
  supa.diff_vs_target,
  supa.rn,
  CASE
    WHEN COALESCE(supa.diff_vs_target, 0) > 0 AND supa.rn = 1 THEN 'valid_progressive_rebate'
  END AS valid_progressive_flag,
  CASE
      WHEN supa.tier_term_rebate_type = 'Absolute' AND COALESCE(supa.diff_vs_target, 0) > 0 AND supa.rn = 1 THEN supa.tier_term_rebate
      WHEN supa.tier_term_rebate_type = 'Percentage' AND COALESCE(supa.diff_vs_target, 0) > 0 AND supa.rn = 1 AND supa.term_frequency = 'Monthly' THEN 
          (supa.tier_term_rebate / 100) * supa.sum_net_amount_current_month_sup_term
      WHEN supa.tier_term_rebate_type = 'Percentage' AND COALESCE(supa.diff_vs_target, 0) > 0 AND supa.rn = 1 AND supa.term_frequency = 'Quarterly' THEN 
          (supa.tier_term_rebate / 100) * supa.sum_net_amount_current_quarter_sup_term 
      WHEN supa.tier_term_rebate_type = 'Percentage' AND COALESCE(supa.diff_vs_target, 0) > 0 AND supa.rn = 1 AND supa.term_frequency IN ('Annually', 'One Time') THEN 
          (supa.tier_term_rebate / 100) * supa.sum_net_amount_current_year_sup_term
      ELSE NULL
  END AS calculated_progressive_rebate,
  DATE('{{ param_month }}') AS month
FROM sup_aggregated_data AS supa
LEFT JOIN suppliers AS sp
  ON supa.country_code = sp.country_code
  AND supa.sup_id = sp.sup_id
