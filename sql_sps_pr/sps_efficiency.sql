-- This table extracts and maintains the efficiency metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 7.2
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_efficiency

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_efficiency`
CLUSTER BY
   global_entity_id, 
   time_period
AS
WITH date_config AS (
  SELECT 
     DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 4 QUARTER) AS lookback_limit
) 
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
    CASE
        WHEN GROUPING(month) = 0 THEN CAST(DATE_SUB(DATE(month), INTERVAL 1 YEAR) AS STRING)
        ELSE CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) - 1 AS STRING))
    END AS last_year_time_period,
    COUNT(DISTINCT sku_id) sku_listed,
    COUNT(DISTINCT CASE WHEN date_diff >=90 THEN sku_id END) AS sku_mature,
    COUNT(DISTINCT CASE WHEN date_diff <90 AND date_diff >30 THEN sku_id END) AS sku_probation,
    COUNT(DISTINCT CASE WHEN date_diff <=30 THEN sku_id END) AS sku_new,
    COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((avg_qty_sold = 0 OR avg_qty_sold IS NULL) AND new_availability = 1)  THEN sku_id END) zero_movers,
    COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((avg_qty_sold = 0 OR avg_qty_sold IS NULL) AND (new_availability < 1 OR new_availability IS NULL))  THEN sku_id END) la_zero_movers,
    COUNT(DISTINCT CASE WHEN date_diff >=90 AND (avg_qty_sold < 1 AND avg_qty_sold > 0) AND (new_availability >= 0.8) THEN sku_id END) slow_movers,
    COUNT(DISTINCT CASE WHEN date_diff >=90 AND (avg_qty_sold < 1 AND avg_qty_sold > 0) AND (new_availability < 0.8 OR new_availability IS NULL)  THEN sku_id END) la_slow_movers,
    COUNT(DISTINCT CASE WHEN date_diff >=90 AND (avg_qty_sold >= 1)  THEN sku_id END) efficient_movers,
    COUNT(DISTINCT CASE WHEN date_diff <30 AND (avg_qty_sold = 0 OR avg_qty_sold IS NULL)  THEN sku_id END) new_zero_movers,
    COUNT(DISTINCT CASE WHEN date_diff <30 AND (avg_qty_sold < 1)  THEN sku_id END) new_slow_movers,
    COUNT(DISTINCT CASE WHEN date_diff <90 AND (avg_qty_sold >= 1)  THEN sku_id END) new_efficient_movers,
    ROUND(SUM(sold_items),1) AS sold_items,
    ROUND(SUM(gpv_eur),1) AS gpv_eur
   FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_efficiency_month`
   WHERE CAST(month AS DATE) >= (SELECT lookback_limit FROM date_config)
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
