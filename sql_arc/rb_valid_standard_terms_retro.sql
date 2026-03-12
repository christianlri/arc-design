--This table filters and aggregates valid contract terms for rebate calculations, ensuring only active terms are included.
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
WITH
 -- ADDED: month scopes for partition pruning (performance only, never business logic)
 year_months AS (
   SELECT m AS month
   FROM UNNEST(GENERATE_DATE_ARRAY(
     DATE_TRUNC(DATE('{{ param_month }}'), YEAR),
     DATE_TRUNC(DATE('{{ param_month }}'), MONTH),
     INTERVAL 1 MONTH
   )) AS m
 ),
 quarter_months AS (
   SELECT m AS month
   FROM UNNEST(GENERATE_DATE_ARRAY(
     DATE_TRUNC(DATE('{{ param_month }}'), QUARTER),
     DATE_TRUNC(DATE('{{ param_month }}'), MONTH),
     INTERVAL 1 MONTH
   )) AS m
 ),
/*ALL TERMS: adding currency information -!! this is the official currency; not the currency from the term table from SF!!*/
currency AS (
 SELECT cr.currency_iso_code AS rebate_currency,
   LOWER(cr.country_code) AS country_code,
 FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_coredata }}.global_entities` AS cr
 GROUP BY ALL
),
/*ALL TERMS: adding category name info (name mapping will be changed ast some point to categ_id mapping) - for term_applicability*/
products AS (
 SELECT p.sku,
   p.sku_name,
   p.barcodes,
   p.country_code,
   p.pim_brand_id,
   p.pim_category_id,
   p.pim_product_id,
   p.brand_name,
   p.brand_name_local,
   p.categ_level_one,
   p.categ_level_two,
   p.categ_level_three,
   p.categ_level_four,
   p.categ_level_five,
   p.categ_level_six,
   p.master_created_at,
   p.categories_all_pim_ids_norm,
 FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_products` AS p
 WHERE TRUE
 AND REGEXP_CONTAINS(p.country_code, {{ params.param_country_code }})
 GROUP BY ALL
),
/* Determine the ideal date range for each term frequency based on param_month. This range will then be intersected with the actual contract term dates.*/
term_periods AS (
 SELECT
   co.contract_term_id,
   co.term_frequency,
   co.country_code,
   CASE
     WHEN REGEXP_CONTAINS(co.country_code, {{ params.param_country_code_accrual }})
     THEN CAST(DATE_TRUNC('{{ param_month }}', MONTH) AS DATETIME)
     WHEN REGEXP_CONTAINS(co.term_frequency, r'Monthly')
     THEN CAST(DATE_TRUNC('{{ param_month }}', MONTH) AS DATETIME)
     WHEN REGEXP_CONTAINS(co.term_frequency, r'Quarterly')
     THEN CAST(DATE_TRUNC('{{ param_month }}', QUARTER) AS DATETIME)
     WHEN REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time')
     THEN CAST(DATE_TRUNC('{{ param_month }}', YEAR) AS DATETIME)
     ELSE NULL
   END AS ideal_report_period_start_datetime,
   CASE
     WHEN REGEXP_CONTAINS(co.country_code, {{ params.param_country_code_accrual }})
     THEN CAST(DATE_ADD(LAST_DAY('{{ param_month }}', MONTH), INTERVAL 1 DAY) AS DATETIME)
     WHEN REGEXP_CONTAINS(co.term_frequency, r'Monthly')
     THEN CAST(DATE_ADD(LAST_DAY('{{ param_month }}', MONTH), INTERVAL 1 DAY) AS DATETIME)
     WHEN REGEXP_CONTAINS(co.term_frequency, r'Quarterly')
     THEN CAST(DATE_ADD(LAST_DAY('{{ param_month }}', QUARTER), INTERVAL 1 DAY) AS DATETIME)
     WHEN REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time')
     THEN CAST(DATE_ADD(LAST_DAY('{{ param_month }}', YEAR), INTERVAL 1 DAY) AS DATETIME)
     ELSE NULL
   END AS ideal_report_period_end_exclusive_datetime,
   month
 FROM
   `{{ params.project_id }}.{{ params.dataset.cl }}.rb_contract_info_retro` AS co
 WHERE TRUE
   AND co.month = '{{ param_month }}'
 GROUP BY
   ALL
),
rb_monthly_sku_amount AS (
 SELECT
   mo.received_local_month,
   mo.country_code,
   mo.sup_id,
   mo.sku,
   mo.brand_name,
   mo.categ_level_one,
   mo.categ_level_two,
   mo.categ_level_three,
   mo.categ_level_four,
   mo.share_sku_in_supplier,
   mo.share_sku_in_brand,
   mo.share_sku_in_categ1,
   mo.share_sku_in_categ2,
   mo.share_sku_in_categ3,
   mo.share_sku_in_categ4,
 FROM
   `{{ params.project_id }}.{{ params.dataset.cl }}.rb_monthly_sku_amount` AS mo
 WHERE TRUE
 AND mo.received_local_month = '{{ param_month }}'
),
/*Calculate adjusted contract start/end dates based on Active-Extended logic*/
adjusted_contract_dates AS (
 SELECT
   co.contract_term_id,
   co.contract_status,
   co.term_frequency,
   co.term_start_date,
   co.term_end_date,
   co.contract_enddate,
   co.contract_effective_enddate,
   /*Adjusted term_start_date logic*/
   CASE
     WHEN (REGEXP_CONTAINS(co.contract_status, r'Active - Extended')
       AND DATE_TRUNC(co.term_start_date, month) < DATE_ADD(DATE_TRUNC('{{ param_month }}', month),INTERVAL -36 MONTH))
         THEN DATE_ADD(co.term_start_date, INTERVAL 3 YEAR)
     WHEN (REGEXP_CONTAINS(co.contract_status, r'Active - Extended')
       AND DATE_TRUNC(co.term_start_date, month) < DATE_ADD(DATE_TRUNC('{{ param_month }}', month),INTERVAL -24 MONTH))
         THEN DATE_ADD(co.term_start_date, INTERVAL 2 YEAR)
     WHEN (REGEXP_CONTAINS(co.contract_status, r'Active - Extended')
       AND DATE_TRUNC(co.term_start_date, month) < DATE_ADD(DATE_TRUNC('{{ param_month }}', month),INTERVAL -12 MONTH))
         THEN DATE_ADD(co.term_start_date, INTERVAL 1 YEAR)
     ELSE co.term_start_date
   END AS effective_contract_start_date,
   /*Adjusted term_end_date logic*/
   CASE
     WHEN (REGEXP_CONTAINS(co.contract_status, r'Active - Extended|Active')
       AND REGEXP_CONTAINS(co.term_frequency, r'Quarterly'))
         THEN DATE_TRUNC(LAST_DAY(DATE_ADD(DATE_TRUNC('{{ param_month }}', QUARTER),INTERVAL 0 QUARTER),QUARTER),MONTH)
     WHEN (REGEXP_CONTAINS(co.contract_status, r'Active - Extended|Active')
       AND REGEXP_CONTAINS(co.term_frequency, r'Monthly'))
         THEN DATE_TRUNC(LAST_DAY(DATE_ADD(DATE_TRUNC('{{ param_month }}', MONTH),INTERVAL 0 MONTH),MONTH),MONTH)
     WHEN (co.contract_status = 'Active'
       AND REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time'))
         THEN DATE_TRUNC(LAST_DAY(DATE_ADD(DATE_TRUNC('{{ param_month }}', YEAR),INTERVAL 0 YEAR),YEAR),MONTH)
     WHEN (REGEXP_CONTAINS(co.contract_status, r'Active - Extended')
       AND REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time'))
         THEN DATE_TRUNC(LAST_DAY(DATE_ADD(DATE_TRUNC('{{ param_month }}', YEAR),INTERVAL 0 YEAR),YEAR),MONTH)
     WHEN co.contract_status = 'Active'
       THEN DATE_TRUNC(co.contract_enddate, MONTH)
     ELSE DATE(DATE_ADD('{{ param_month }}', INTERVAL 0 DAY))
   END AS effective_contract_end_date,
   co.month
 FROM
   `{{ params.project_id }}.{{ params.dataset.cl }}.rb_contract_info_retro` AS co
 WHERE co.month = '{{ param_month }}'
),
all_rebates AS (
SELECT
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.source_table END AS source_table,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.warehouse_type END AS warehouse_type,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.warehouse_id END AS warehouse_id,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.warehouse_name END AS warehouse_name,
     cr.rebate_currency,
     co.term_currency,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.fixing_condition END AS fixing_condition,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.reason END AS reason,
     co.contract_id,
     co.opp_id,
     co.account_id,
     co.contract_term_id,
     co.opp_term_id,
     co.global_entity_id,
     co.country_code,
     co.country,
     co.global_supplier_id,
     co.supplier_name,
     co.finance_system_id,
     co.sup_id,
     co.unit_of_measure,
     co.trading_term_name,
     co.trading_term_cluster,
     co.trading_term_type,
     co.rebate_term_value,
     co.term_value_type,
     co.term_start_date,
     co.term_end_date,
     co.term_applicability,
     co.term_frequency,
     co.term_brand_name,
     co.term_comments,
     co.term_pim_brand_id,
     co.brand_category_pim,
     co.term_brand_category_name,
     co.contract_startdate,
     co.contract_enddate,
     co.contract_effective_enddate,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.order_id END AS order_id,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.order_type END AS order_type,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE ol.po_id END AS po_id,
     ol.sku,
     p.sku_name,
     ol.received_local_time,
     p.brand_name AS sku_brand_name,
     p.categ_level_one AS sku_categ_level_one,
     p.categ_level_two AS sku_categ_level_two,
     p.categ_level_three AS sku_level_three,
     p.categ_level_four AS sku_level_four,
     p.categ_level_five AS sku_level_five,
     p.categ_level_six AS sku_level_six,
     CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN 'absolute_term' ELSE p.barcodes END AS barcodes,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.gross_cost_without_vat END, 0) AS gross_cost_without_vat,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.gross_cost END, 0) AS gross_cost,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.ordered_quantity END, 0) AS ordered_quantity,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.delivered_quantity END, 0) AS delivered_quantity,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.returned_quantity END, 0) AS returned_quantity,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.trade_cost END, 0) AS trade_cost,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.fixed_discount END, 0) AS fixed_discount,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.extra_discount END, 0) AS extra_discount,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.fixed_vat END, 0) AS fixed_vat,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.extra_vat END, 0) AS extra_vat,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.vat_rate END, 0) AS vat_rate,
     IFNULL(CASE WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute') THEN NULL ELSE ol.internal_tax END, 0) AS internal_tax,
     co.contract_status,
     co.contract_term_type,
     co.account_owner,
     CASE
       WHEN REGEXP_CONTAINS(ol.order_type, 'regular|last_updated_po') THEN 1
       ELSE -1
     END AS rebate_month_condition,
     co.tier_term_rebate_type,
     co.tier_term_rebate,
     co.tier_term_threshold,
     co.tier_term_number,
     mo.share_sku_in_supplier,
     mo.share_sku_in_brand,
     mo.share_sku_in_categ1,
     mo.share_sku_in_categ2,
     mo.share_sku_in_categ3,
     mo.share_sku_in_categ4,
     /*Calculate the effective term start/end dates by intersecting adjusted contract dates with ideal report period*/
     GREATEST(acd.effective_contract_start_date, DATE(tp.ideal_report_period_start_datetime)) AS calculated_term_start_date,
     DATE(LEAST(acd.effective_contract_end_date, DATE(tp.ideal_report_period_end_exclusive_datetime))) AS calculated_term_end_date,
           -- NEW: Flag to determine if param_month falls into the final month of the term's frequency period
     -- This ensures rebates for Quarterly/Annually terms are calculated only in the last month of that period.
     CASE
       WHEN REGEXP_CONTAINS(co.country_code, {{ params.param_country_code_accrual }}) THEN TRUE
       WHEN REGEXP_CONTAINS(co.term_frequency, r'Monthly')
         AND DATE_TRUNC('{{ param_month }}', MONTH) = DATE_TRUNC(LAST_DAY('{{ param_month }}', MONTH), MONTH)
           THEN TRUE
       WHEN REGEXP_CONTAINS(co.term_frequency, r'Quarterly')
         AND DATE_TRUNC('{{ param_month }}', MONTH) = DATE_TRUNC(LAST_DAY('{{ param_month }}', QUARTER), MONTH)
           THEN TRUE
       WHEN REGEXP_CONTAINS(co.term_frequency, r'Annually|One Time')
         AND DATE_TRUNC('{{ param_month }}', MONTH) = DATE_TRUNC(LAST_DAY('{{ param_month }}', YEAR), MONTH)
           THEN TRUE
       ELSE FALSE
     END AS is_final_month_of_period,
     /*this is a calculation for adjustment of term frequency*/
     SUM(CAST(IFNULL(ol.gross_cost, 0) AS FLOAT64) * CAST(IFNULL(ol.delivered_quantity, 0) AS FLOAT64)) AS calc_gross_delivered,
     SUM(CAST(IFNULL(ol.gross_cost, 0) AS FLOAT64) * CAST(IFNULL(ol.returned_quantity, 0) AS FLOAT64)) AS calc_gross_return,
     SUM(CAST(IFNULL(ol.gross_cost_without_vat, 0) AS FLOAT64) * CAST(IFNULL(ol.delivered_quantity, 0) AS FLOAT64)) AS calc_net_delivered,
     SUM(CAST(IFNULL(ol.gross_cost_without_vat, 0) AS FLOAT64) * CAST(IFNULL(ol.returned_quantity, 0) AS FLOAT64)) AS calc_net_return,
     CASE
       WHEN REGEXP_CONTAINS( co.global_entity_id, {{ params.distribution_param_global_entity_id }})
         AND REGEXP_CONTAINS(LOWER(co.term_value_type), r'percentage')
         AND REGEXP_CONTAINS(LOWER(co.trading_term_name), r'distribution allowance')
         AND REGEXP_CONTAINS(LOWER(ol.warehouse_type), r'dmart') THEN 0
       /*this is for trading_term_name = 'distrub allowance' with  warehouse= 'dmart'*/
       WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute')
         AND co.term_applicability = 'Brand'
         AND (LOWER(co.brand_category_pim) = LOWER(p.pim_brand_id)
           OR LOWER(co.term_brand_category_name) = LOWER(p.brand_name)
           OR LOWER(co.term_brand_category_name) = LOWER(p.brand_name_local))
             THEN ROUND(CAST(co.rebate_term_value AS FLOAT64) * CAST(mo.share_sku_in_brand AS FLOAT64),2)
       WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute')
         AND co.term_applicability = 'Category'
         AND EXISTS (
           SELECT 1
           FROM UNNEST(p.categories_all_pim_ids_norm) AS cat JOIN UNNEST(SPLIT(NORMALIZE_AND_CASEFOLD(TRIM(CAST(co.brand_category_pim AS STRING))), ',')) AS c
           ON c = cat
         )
           THEN ROUND(CAST(co.rebate_term_value AS FLOAT64) * CAST(COALESCE(mo.share_sku_in_categ1, mo.share_sku_in_categ2, mo.share_sku_in_categ3, mo.share_sku_in_categ4)AS FLOAT64),2)
       WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute')
         AND REGEXP_CONTAINS(LOWER(co.term_applicability), r'all master product')
           THEN ROUND(CAST(co.rebate_term_value AS FLOAT64) * CAST(mo.share_sku_in_supplier AS FLOAT64),2)
       WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute')
         AND REGEXP_CONTAINS(LOWER(co.term_applicability), r'specific master product')
         AND LOWER(p.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(co.brand_category_pim), ',')) as sku)
           THEN CAST(co.rebate_term_value AS FLOAT64)
       WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute')
         AND REGEXP_CONTAINS(LOWER(co.term_applicability), r'all other master product')
         AND COALESCE(LOWER(co.brand_category_pim) = LOWER(p.pim_brand_id)
           OR LOWER(co.term_brand_category_name) = LOWER(p.brand_name)
           OR LOWER(co.term_brand_category_name) = LOWER(p.brand_name_local), TRUE)
           THEN ROUND(CAST(co.rebate_term_value AS FLOAT64) * CAST(mo.share_sku_in_brand AS FLOAT64),2)
       WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute')
         AND REGEXP_CONTAINS(LOWER(co.term_applicability), r'all other master product')
         AND EXISTS (
           SELECT 1
           FROM UNNEST(p.categories_all_pim_ids_norm) AS cat JOIN UNNEST(SPLIT(NORMALIZE_AND_CASEFOLD(TRIM(CAST(co.brand_category_pim AS STRING))), ',')) AS c
           ON c = cat
         )
           THEN ROUND(CAST(co.rebate_term_value AS FLOAT64) * CAST(COALESCE(mo.share_sku_in_categ1, mo.share_sku_in_categ2, mo.share_sku_in_categ3, mo.share_sku_in_categ4)AS FLOAT64),2)
       WHEN REGEXP_CONTAINS(LOWER(co.term_value_type), r'absolute')
         AND REGEXP_CONTAINS(LOWER(co.term_applicability), r'all other master product')
         AND LOWER(p.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(co.brand_category_pim), ',')) as sku)
         THEN ROUND(CAST(co.rebate_term_value AS FLOAT64) * CAST(mo.share_sku_in_supplier AS FLOAT64),2)
       ELSE
       /*this is the rest of percentages cases*/
       (
         (
           CAST(IFNULL(ol.gross_cost_without_vat, 0) AS FLOAT64) * CAST(IFNULL(ol.delivered_quantity, 0) AS FLOAT64)
         ) - (
           CAST(IFNULL(ol.gross_cost_without_vat, 0) AS FLOAT64) * CAST(IFNULL(ol.returned_quantity, 0) AS FLOAT64)
         )
       ) * (CAST(co.rebate_term_value AS FLOAT64) / 100) * (
         CASE
           WHEN REGEXP_CONTAINS(
             ol.order_type,
             'regular|last_updated_po|updated_next_months|stock_movement|return'
           ) THEN 1
           ELSE -1
         END
       )
       /*this is to identify the order from previous month which should be subtracted -> (-1)*/
     END AS standard_rebate,
      /*valid_rebate column it helps us to mark as 'valid' only the rebates that should be considered for the final calculation; for the cases where the term_applicability are at the brand, categ or sku level*/
     CASE
       WHEN co.term_applicability = 'Brand'
         AND (LOWER(co.brand_category_pim) = LOWER(p.pim_brand_id)
           OR LOWER(co.term_brand_category_name) = LOWER(p.brand_name)
           OR LOWER(co.term_brand_category_name) = LOWER(p.brand_name_local))
         THEN '2_valid_brand'
       WHEN co.term_applicability = 'Category'
         AND EXISTS ( SELECT 1
         FROM UNNEST(p.categories_all_pim_ids_norm) AS cat
         JOIN UNNEST(SPLIT(NORMALIZE_AND_CASEFOLD(TRIM(CAST(co.brand_category_pim AS STRING))), ',') ) AS c ON c = cat )THEN '3_valid_categ'
       WHEN REGEXP_CONTAINS(LOWER(co.term_applicability), r'all master product') THEN '1_valid_sup'
       WHEN REGEXP_CONTAINS(LOWER(co.term_applicability), r'specific master product')
         AND LOWER(p.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(co.brand_category_pim), ',')) as sku) THEN '4_valid_sku'
       WHEN REGEXP_CONTAINS(LOWER(co.term_applicability), r'all other master product')
         AND (COALESCE(LOWER(co.brand_category_pim) = LOWER(p.pim_brand_id)
           OR LOWER(co.term_brand_category_name) = LOWER(p.brand_name)
           OR LOWER(co.term_brand_category_name) = LOWER(p.brand_name_local), TRUE)
           OR COALESCE(LOWER(co.brand_category_pim) = LOWER(p.pim_category_id), TRUE)
           OR LOWER(p.pim_product_id) IN (SELECT sku FROM UNNEST(SPLIT(LOWER(co.brand_category_pim), ',')) as sku)
         ) THEN '5_valid_other_skus'
       ELSE 'not_valid'
     END AS valid_rebate,
     /*valid_rebate column it helps us to mark as 'valid' only the rebates that should be considered for the final calculation; for the cases where the term_applicability are at the brand, categ or sku level*/
     ol.excise_tax
   FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_orderline_sku` AS ol
   JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.rb_contract_info_retro` AS co
     ON co.sup_id = ol.sup_id
     AND co.country_code = ol.country_code
     AND co.trading_term_cluster = 'standard_terms'
        AND ((co.contract_status = 'Active'
              AND (ol.received_local_time BETWEEN co.term_start_date AND COALESCE (co.term_end_date,COALESCE(co.contract_effective_enddate,co.contract_enddate))))
          OR co.contract_status = 'Active - Extended')
     AND co.month = '{{ param_month }}'
   /* only the relevant active contracts */
   JOIN term_periods AS tp
     ON tp.contract_term_id = co.contract_term_id
     AND ol.received_local_time >= tp.ideal_report_period_start_datetime
     AND ol.received_local_time < tp.ideal_report_period_end_exclusive_datetime
   JOIN adjusted_contract_dates AS acd
     ON acd.contract_term_id = co.contract_term_id
     AND acd.month = co.month
   LEFT JOIN currency AS cr
     ON co.country_code = cr.country_code
   LEFT JOIN products AS p
     ON ol.sku = p.sku
     AND ol.country_code = p.country_code
   LEFT JOIN rb_monthly_sku_amount AS mo
     ON ol.country_code = mo.country_code
     AND ol.sup_id = mo.sup_id
     AND ol.sku = mo.sku
     AND ol.month = mo.received_local_month
   WHERE TRUE
    AND (
       -- EXCEPTION ACCRUAL
       (REGEXP_CONTAINS(co.country_code, {{ params.param_country_code_accrual }}) AND ol.month = DATE('{{ param_month }}'))
       OR
       -- original logic for other countries with monthly/quarterly/annually frequencies
       (NOT REGEXP_CONTAINS(co.country_code, {{ params.param_country_code_accrual }}) AND REGEXP_CONTAINS(LOWER(co.term_frequency), r'monthly') AND ol.month = DATE('{{ param_month }}')) 
       OR
       (NOT REGEXP_CONTAINS(co.country_code, {{ params.param_country_code_accrual }}) AND REGEXP_CONTAINS(LOWER(co.term_frequency), r'quarterly') AND ol.month IN (SELECT month FROM quarter_months)) 
       OR
       (NOT REGEXP_CONTAINS(co.country_code, {{ params.param_country_code_accrual }}) AND REGEXP_CONTAINS(LOWER(co.term_frequency), r'annually|one time') AND ol.month IN (SELECT month FROM year_months))
     )
   GROUP BY ALL
 )
SELECT
 *,
 DATE('{{ param_month }}') AS month
FROM (
 SELECT *
   EXCEPT (valid_rebate, standard_rebate),
     CASE WHEN is_final_month_of_period THEN
       CASE WHEN REGEXP_CONTAINS(LOWER(term_value_type), r'absolute') THEN
         CASE WHEN ROW_NUMBER() OVER (
           PARTITION BY global_entity_id, sup_id, sku, term_value_type, contract_term_id, contract_id, valid_rebate
           ORDER BY order_id, received_local_time ASC
         ) = 1 THEN standard_rebate
         ELSE 0
         END
       ELSE standard_rebate
       END
     ELSE 0
     END AS rebate,
     /* This filter was necessary to exclude the cases where the rebate should not be considered for the 'all other master product' category. */
     CASE WHEN REGEXP_CONTAINS(LOWER(valid_rebate), r'1_valid_sup') THEN 0
       WHEN REGEXP_CONTAINS(LOWER(valid_rebate), r'5_valid_other_skus') THEN
       RANK() OVER (
         PARTITION BY country_code, source_table, sku, order_id, contract_id, trading_term_name, trading_term_type
         ORDER BY valid_rebate)
       ELSE
       RANK() OVER (
         PARTITION BY country_code, source_table, sku, order_id, contract_id, contract_term_id
         ORDER BY valid_rebate
       ) END AS rank_valid_rebate
FROM all_rebates
WHERE TRUE
 AND valid_rebate <> 'not_valid')
WHERE TRUE
 AND rank_valid_rebate <= 1
 AND rebate <> 0
GROUP BY ALL
