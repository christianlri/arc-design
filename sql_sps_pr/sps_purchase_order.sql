-- This table extracts and maintains the order metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 6.2
-- DML SCRIPT: SPS Refact Full Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_purchase_order
CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_purchase_order`
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
     COUNT(DISTINCT(CASE WHEN order_received_on_time = 1 AND is_compliant_flag THEN po_order_id END)) as on_time_orders,
     COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
     COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL AND is_compliant_flag THEN po_order_id END)) AS total_non_cancelled__po_orders,
     COALESCE(SUM((CASE WHEN order_status = 'done' AND is_compliant_flag THEN total_received_qty_per_order END)),0) AS total_received_qty_per_po_order,
     COALESCE(SUM((CASE WHEN order_status = 'done' AND is_compliant_flag THEN total_demanded_qty_per_order END)),0) AS total_demanded_qty_per_po_order,
     ROUND(SAFE_DIVIDE(COALESCE(SUM((CASE WHEN order_status = 'done' AND is_compliant_flag THEN total_received_qty_per_order END)),0), COALESCE(SUM((CASE WHEN order_status = 'done' AND is_compliant_flag THEN total_demanded_qty_per_order END)),0)), 4) AS fill_rate,
     ROUND(SAFE_DIVIDE(COUNT(DISTINCT(CASE WHEN order_received_on_time = 1 AND is_compliant_flag THEN po_order_id END)), COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL AND is_compliant_flag THEN po_order_id END))), 4 ) AS otd,
     COALESCE(SUM((CASE WHEN cancel_reason IN  ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available') THEN total_demanded_qty_per_order END)),0) AS supplier_non_fulfilled_order_qty,
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_purchase_order_month`
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
