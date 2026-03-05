-- This table extracts and maintains the financial metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 5.3
-- DML SCRIPT: SPS Refact Full Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_financial_metrics

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_financial_metrics`
CLUSTER BY
   global_entity_id,
   time_period
AS 
WITH current_year_data AS (
  SELECT
    global_entity_id,
    CASE WHEN GROUPING(month) = 0 THEN CAST(month AS STRING) ELSE quarter_year END AS time_period,
    CASE 
        WHEN GROUPING(principal_supplier_id) = 0 THEN principal_supplier_id
        WHEN GROUPING(supplier_id) = 0 THEN supplier_id
        WHEN GROUPING(brand_owner_name) = 0 THEN brand_owner_name
        WHEN GROUPING(brand_name) = 0 THEN brand_name
        ELSE 'total'
    END AS brand_sup,
    COALESCE(
      IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
      IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
      IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
      IF(GROUPING(brand_name) = 0, brand_name, NULL),
      IF(GROUPING(brand_owner_name) = 0, brand_owner_name, NULL),
      IF(GROUPING(supplier_id) = 0, supplier_id, NULL),
      principal_supplier_id
    ) AS entity_key,
    CASE 
        WHEN GROUPING(principal_supplier_id) = 0 THEN 'principal' 
        WHEN GROUPING(supplier_id) = 0 THEN 'division' 
        WHEN GROUPING(brand_owner_name) = 0 THEN 'brand_owner' 
        WHEN GROUPING(brand_name) = 0 THEN 'brand_name'
        ELSE 'total'
    END AS division_type,
    CASE 
        WHEN GROUPING(l3_master_category) = 0 THEN 'level_three' 
        WHEN GROUPING(l2_master_category) = 0 THEN 'level_two' 
        WHEN GROUPING(l1_master_category) = 0 THEN 'level_one' 
        WHEN GROUPING(brand_name) = 0 THEN 'brand_name'
        ELSE 'supplier' 
    END AS supplier_level,
    CASE WHEN GROUPING(month) = 0 THEN 'Monthly' ELSE 'Quarterly' END AS time_granularity,
    COUNT(DISTINCT analytical_customer_id) AS total_customers,
    COUNT(DISTINCT sku_id) AS total_skus_sold,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT warehouse_id) AS total_warehouses_sold,
    ------------ EUR ----------------------------------------
    CAST(ROUND(IFNULL(SUM(amt_total_price_paid_net_eur),0), 2) AS NUMERIC) AS Total_Net_Sales_eur_order,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2) AS NUMERIC) AS Net_Sales_eur,
    CAST(ROUND(IFNULL(SUM(COGS_eur),0),4) AS NUMERIC) AS COGS_eur,
    CAST(ROUND(IFNULL(SUM(CASE WHEN unit_discount_amount_eur > 0 THEN total_price_paid_net_eur END),0),2) AS NUMERIC) AS Net_Sales_from_promo_eur,
    CAST(ROUND(IFNULL(SUM(total_supplier_funding_eur),0),2) AS NUMERIC) AS total_supplier_funding_eur,
    CAST(ROUND((SUM(total_price_paid_net_eur) - SUM(COGS_eur)), 2) AS NUMERIC) AS front_margin_amt_eur,
    CAST(ROUND(IFNULL(SUM(total_discount_eur),0),4) AS NUMERIC) AS total_discount_eur,
    CAST(ROUND(IFNULL(SAFE_DIVIDE(ROUND(IFNULL(SUM(CASE WHEN unit_discount_amount_eur > 0 THEN total_price_paid_net_eur END),0),2), ROUND(IFNULL(SUM(total_price_paid_net_eur),0), 2)), 0), 4) AS NUMERIC) AS Promo_GPV_contribution_eur,
    ------------ LC ----------------------------------------
    CAST(ROUND(IFNULL(SUM(amt_total_price_paid_net_lc),0), 2) AS NUMERIC) AS Total_Net_Sales_lc_order,
    CAST(ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2) AS NUMERIC) AS Net_Sales_lc,
    CAST(ROUND(IFNULL(SUM(COGS_lc),0), 4) AS NUMERIC) AS COGS_lc,
    CAST(ROUND(IFNULL(SUM(CASE WHEN unit_discount_amount_lc > 0 THEN total_price_paid_net_lc END),0),2) AS NUMERIC) AS Net_Sales_from_promo_lc,
    CAST(ROUND(IFNULL(SUM(total_supplier_funding_lc),0),2) AS NUMERIC) AS total_supplier_funding_lc,
    CAST(ROUND((SUM(total_price_paid_net_lc) - SUM(COGS_lc)), 2) AS NUMERIC) AS front_margin_amt_lc,
    CAST(ROUND(IFNULL(SUM(total_discount_lc),0), 4) AS NUMERIC) AS total_discount_lc,
    CAST(ROUND(IFNULL(SAFE_DIVIDE(ROUND(IFNULL(SUM(CASE WHEN unit_discount_amount_lc > 0 THEN total_price_paid_net_lc END),0),2), ROUND(IFNULL(SUM(total_price_paid_net_lc),0), 2)), 0), 4) AS NUMERIC) AS Promo_GPV_contribution_lc,
    ------- Other aggregated metrics ---------------
    CAST(ROUND(SUM (amt_gbv_eur),2) AS NUMERIC) AS total_GBV,
    CAST(ROUND(SUM (fulfilled_quantity),2) AS NUMERIC) AS fulfilled_quantity
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_financial_metrics_month`
  -- Pull data for the last 4 quarters (1 year)
  WHERE DATE(month) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 4 QUARTER)
GROUP BY GROUPING SETS (
    -- ==========================================================
    -- MONTHLY BREAKDOWNS (month)
    -- ==========================================================
    
    -- 1. TOTAL OWNER LEVEL (No Category/Brand Deep-dive)
    (month, global_entity_id, principal_supplier_id),
    (month, global_entity_id, supplier_id),
    (month, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE (By Owner + Brand Name)
    (month, global_entity_id, principal_supplier_id, brand_name),
    (month, global_entity_id, supplier_id, brand_name),
    (month, global_entity_id, brand_owner_name, brand_name),
    (month, global_entity_id, brand_name), 

    -- 3. CATEGORY DEEP-DIVE (By Owner + Categories)
    (month, global_entity_id, principal_supplier_id, l1_master_category),
    (month, global_entity_id, principal_supplier_id, l2_master_category),
    (month, global_entity_id, principal_supplier_id, l3_master_category),

    (month, global_entity_id, supplier_id, l1_master_category),
    (month, global_entity_id, supplier_id, l2_master_category),
    (month, global_entity_id, supplier_id, l3_master_category),

    (month, global_entity_id, brand_owner_name, l1_master_category),
    (month, global_entity_id, brand_owner_name, l2_master_category),
    (month, global_entity_id, brand_owner_name, l3_master_category),

    (month, global_entity_id, brand_name, l1_master_category),
    (month, global_entity_id, brand_name, l2_master_category),
    (month, global_entity_id, brand_name, l3_master_category),
    -- ==========================================================
    -- QUARTERLY BREAKDOWNS (quarter_year)
    -- ==========================================================
    
    -- 1. TOTAL OWNER LEVEL
    (quarter_year, global_entity_id, principal_supplier_id),
    (quarter_year, global_entity_id, supplier_id),
    (quarter_year, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE
    (quarter_year, global_entity_id, principal_supplier_id, brand_name),
    (quarter_year, global_entity_id, supplier_id, brand_name),
    (quarter_year, global_entity_id, brand_owner_name, brand_name),
    (quarter_year, global_entity_id, brand_name),

    -- 3. CATEGORY DEEP-DIVE
    (quarter_year, global_entity_id, principal_supplier_id, l1_master_category),
    (quarter_year, global_entity_id, principal_supplier_id, l2_master_category),
    (quarter_year, global_entity_id, principal_supplier_id, l3_master_category),

    (quarter_year, global_entity_id, supplier_id, l1_master_category),
    (quarter_year, global_entity_id, supplier_id, l2_master_category),
    (quarter_year, global_entity_id, supplier_id, l3_master_category),

    (quarter_year, global_entity_id, brand_owner_name, l1_master_category),
    (quarter_year, global_entity_id, brand_owner_name, l2_master_category),
    (quarter_year, global_entity_id, brand_owner_name, l3_master_category), 

    (quarter_year, global_entity_id, brand_name, l1_master_category),
    (quarter_year, global_entity_id, brand_name, l2_master_category),
    (quarter_year, global_entity_id, brand_name, l3_master_category)
)
)

-- Final Merge
SELECT
  cy.*,
  COALESCE(ly.Net_Sales_eur_LY, 0.0) AS Net_Sales_eur_Last_Year,
  COALESCE(ly.Net_Sales_lc_LY, 0.0) AS Net_Sales_lc_Last_Year,
  -- Calculations
  CAST(ROUND(SAFE_DIVIDE(cy.Net_Sales_eur - ly.Net_Sales_eur_LY, NULLIF(ly.Net_Sales_eur_LY, 0)), 3) AS NUMERIC) AS YoY_GPV_Growth_eur,
  CAST(ROUND(SAFE_DIVIDE(cy.Net_Sales_lc - ly.Net_Sales_lc_LY, NULLIF(ly.Net_Sales_lc_LY, 0)), 3) AS NUMERIC) AS YoY_GPV_Growth_lc,
  r.total_rebate AS back_margin_amt_lc,
  COALESCE(r.total_rebate_wo_dist_allowance_lc, 0.0) AS back_margin_wo_dist_allowance_amt_lc,
  CAST(ROUND(SAFE_DIVIDE(cy.Net_Sales_lc + cy.total_supplier_funding_lc - cy.COGS_lc +  COALESCE(r.total_rebate, 0.0), NULLIF(cy.Net_Sales_lc, 0)), 4) AS NUMERIC) AS Total_Margin_LC,
  CAST(ROUND(SAFE_DIVIDE(cy.Net_Sales_eur + cy.total_supplier_funding_eur - cy.COGS_eur , NULLIF(cy.Net_Sales_eur, 0)), 4) AS NUMERIC) AS Front_Margin_eur,
  CAST(ROUND(SAFE_DIVIDE(cy.Net_Sales_lc + cy.total_supplier_funding_lc - cy.COGS_lc , NULLIF(cy.Net_Sales_lc, 0)), 4) AS NUMERIC) AS Front_Margin_lc,
FROM current_year_data cy
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_financial_metrics_prev_year` ly
  ON cy.global_entity_id = ly.global_entity_id
  AND cy.brand_sup = ly.brand_sup
  AND cy.entity_key = ly.entity_key
  AND cy.division_type = ly.division_type
  AND cy.supplier_level = ly.supplier_level
  AND cy.time_period = ly.join_time_period
  AND cy.time_granularity = ly.time_granularity
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_line_rebate_metrics` AS r
  ON cy.global_entity_id = r.global_entity_id
  AND cy.brand_sup = r.brand_sup
  AND cy.entity_key = r.entity_key
  AND cy.division_type = r.division_type
  AND cy.supplier_level = r.supplier_level
  AND cy.time_period = r.time_period
  AND cy.time_granularity = r.time_granularity
