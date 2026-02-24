-- This table extracts and maintains the efficiency metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 8.2
-- DML SCRIPT: SPS Refact Full Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_price_index

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_price_index`
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
    CASE WHEN GROUPING(price_index_month) = 0 THEN CAST(price_index_month AS STRING)  ELSE price_index_quarter_year END AS time_period,
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
    CASE WHEN GROUPING(price_index_month) = 0 THEN 'Monthly' ELSE 'Quarterly' END AS time_granularity,
    ROUND(SAFE_DIVIDE(SUM(median_bp_index * sku_gpv_eur), SUM(sku_gpv_eur)), 2) AS median_price_index,
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_price_index_month`
  WHERE CAST(price_index_month AS DATE) >= (SELECT lookback_limit FROM date_config)
  GROUP BY GROUPING SETS (
    -- ==========================================================
    -- MONTHLY BREAKDOWNS (month)
    -- ==========================================================
    
    -- 1. TOTAL OWNER LEVEL (No Category/Brand Deep-dive)
    (price_index_month, global_entity_id, principal_supplier_id),
    (price_index_month, global_entity_id, supplier_id),
    (price_index_month, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE (By Owner + Brand Name)
    (price_index_month, global_entity_id, principal_supplier_id, brand_name),
    (price_index_month, global_entity_id, supplier_id, brand_name),
    (price_index_month, global_entity_id, brand_owner_name, brand_name),
    (price_index_month, global_entity_id, brand_name), 

    -- 3. CATEGORY DEEP-DIVE (By Owner + Categories)
    (price_index_month, global_entity_id, principal_supplier_id, l1_master_category),
    (price_index_month, global_entity_id, principal_supplier_id, l2_master_category),
    (price_index_month, global_entity_id, principal_supplier_id, l3_master_category),

    (price_index_month, global_entity_id, supplier_id, l1_master_category),
    (price_index_month, global_entity_id, supplier_id, l2_master_category),
    (price_index_month, global_entity_id, supplier_id, l3_master_category),

    (price_index_month, global_entity_id, brand_owner_name, l1_master_category),
    (price_index_month, global_entity_id, brand_owner_name, l2_master_category),
    (price_index_month, global_entity_id, brand_owner_name, l3_master_category),

    (price_index_month, global_entity_id, brand_name, l1_master_category),
    (price_index_month, global_entity_id, brand_name, l2_master_category),
    (price_index_month, global_entity_id, brand_name, l3_master_category),
    -- ==========================================================
    -- QUARTERLY BREAKDOWNS (quarter_year)
    -- ==========================================================
    
    -- 1. TOTAL OWNER LEVEL
    (price_index_quarter_year, global_entity_id, principal_supplier_id),
    (price_index_quarter_year, global_entity_id, supplier_id),
    (price_index_quarter_year, global_entity_id, brand_owner_name),

    -- 2. BRAND DEEP-DIVE
    (price_index_quarter_year, global_entity_id, principal_supplier_id, brand_name),
    (price_index_quarter_year, global_entity_id, supplier_id, brand_name),
    (price_index_quarter_year, global_entity_id, brand_owner_name, brand_name),
    (price_index_quarter_year, global_entity_id, brand_name),

    -- 3. CATEGORY DEEP-DIVE
    (price_index_quarter_year, global_entity_id, principal_supplier_id, l1_master_category),
    (price_index_quarter_year, global_entity_id, principal_supplier_id, l2_master_category),
    (price_index_quarter_year, global_entity_id, principal_supplier_id, l3_master_category),

    (price_index_quarter_year, global_entity_id, supplier_id, l1_master_category),
    (price_index_quarter_year, global_entity_id, supplier_id, l2_master_category),
    (price_index_quarter_year, global_entity_id, supplier_id, l3_master_category),

    (price_index_quarter_year, global_entity_id, brand_owner_name, l1_master_category),
    (price_index_quarter_year, global_entity_id, brand_owner_name, l2_master_category),
    (price_index_quarter_year, global_entity_id, brand_owner_name, l3_master_category), 

    (price_index_quarter_year, global_entity_id, brand_name, l1_master_category),
    (price_index_quarter_year, global_entity_id, brand_name, l2_master_category),
    (price_index_quarter_year, global_entity_id, brand_name, l3_master_category)
)
