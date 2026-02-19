-- This table extracts and maintains the supplier mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 2
-- DML SCRIPT: SPS Refact Full Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_product
-- 1. DEFINE DYNAMIC DATE RANGES
--------------------------------------------------------------------------------
-- Current Month Anchor (M): The last completed day of the previous month
DECLARE run_anchor_date DATE DEFAULT DATE_SUB(DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH), INTERVAL 1 DAY);
-- Last 24 Months Start: Exactly 2 years before the anchor month starts (23 months prior to month start)
DECLARE date_start_current DATE DEFAULT DATE_SUB(DATE_TRUNC(run_anchor_date, MONTH), INTERVAL 23 MONTH);
-- FULL REFRESH: Overwrites the entire sps_product table on every run
CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product`
CLUSTER BY
 global_entity_id,
 warehouse_id,
 supplier_id
AS
WITH
pim_products AS (
 SELECT
     pp.product_id AS pim_product_id,
     COALESCE(NULLIF(LOWER(pp.brands_name), 'unbranded'), '_unknown_') AS brand_name_pim,
     COALESCE(NULLIF(LOWER(pp.brand_owner_name), 'unbranded'), '_unknown_') AS brand_owner_name,
     CASE 
       WHEN LOWER(pp.brands_name) = 'unbranded' OR pp.brands_name IS NULL THEN '_unknown_'
       ELSE REGEXP_REPLACE(LOWER(pp.brands_name), r'[ _\-&/\\()\[\]\.\,\+]', '') 
        END AS clean_brand_name,
 FROM
     `{{ params.project_id }}.{{ params.dataset.cl }}.pim_product` AS pp
 WHERE
     TRUE
 QUALIFY
     ROW_NUMBER() OVER (
         PARTITION BY
             pp.product_id,
             pp.brand_owner_name
         ORDER BY
             (pp.brand_owner_name IS NOT NULL) DESC,
             pp.product_updated_at DESC,
             pp.brands_name
     ) = 1
),
---
-- CTE: Selects the Best Brand Owner Name at the Brand Level (Fallback)
---
pim_brands AS (
 SELECT
     COALESCE(NULLIF(LOWER(pp.brand_owner_name), 'unbranded'), '_unknown_') AS brand_owner_name_brand,
     CASE 
       WHEN LOWER(pp.brands_name) = 'unbranded' OR pp.brands_name IS NULL THEN '_unknown_'
       ELSE REGEXP_REPLACE(LOWER(pp.brands_name), r'[ _\-&/\\()\[\]\.\,\+]', '') 
        END AS clean_brand_name,
 FROM
     `{{ params.project_id }}.{{ params.dataset.cl }}.pim_product` AS pp
 WHERE
     pp.brand_owner_name IS NOT NULL
     AND pp.brands_name IS NOT NULL
 QUALIFY
     -- Select the best brand owner name for a given brand
     ROW_NUMBER() OVER (
         PARTITION BY
             REGEXP_REPLACE(LOWER(pp.brands_name), r'[ _\-&/\\()\[\]\.\,\+]', '') -- Partition by clean_brand_name
         ORDER BY
             pp.product_updated_at DESC, -- Use latest updated product as proxy
             pp.brand_owner_name -- Arbitrary tie-breaker
     ) = 1
),
---
-- CTE : Extracts and Categorizes Product Data from qc_catalog_products
---
qc_catalog_products AS (
 SELECT
     qcp.sku,
     MAX(qcp.product_name) AS sku_name,
     qcp.pim_product_id,
     qcp.global_entity_id,
     qcp.country_code,
     vp.warehouse_id,
     COALESCE(NULLIF(LOWER(qcp.brand_name), 'unbranded'), '_unknown_') AS brand_name,
     CASE 
        WHEN LOWER(qcp.brand_name) = 'unbranded' OR qcp.brand_name IS NULL THEN '_unknown_'
        ELSE REGEXP_REPLACE(LOWER(qcp.brand_name), r'[ _\-&/\\()\[\]\.\,\+]', '') 
            END AS clean_brand_name,
     qcp.catalog_region AS region_code,
     -- LEVEL_ZERO calculation
     CASE
         WHEN LOWER(mc.master_category_names.level_one) IN ('bws') THEN 'BWS'
         WHEN mc.master_category_names.level_one IN ('Bread / Bakery', 'Dairy / Chilled / Eggs') THEN 'Fresh'
         WHEN mc.master_category_names.level_one IN ('General Merchandise') THEN 'General Merchandise'
         WHEN mc.master_category_names.level_one IN ('Beverages', 'Snacks') THEN 'Impulse'
         WHEN mc.master_category_names.level_one IN ('Home / Pet', 'Personal Care / Baby / Health', 'Smoking / Tobacco') THEN 'Non-Food Grocery'
         WHEN mc.master_category_names.level_one IN ('Frozen', 'Packaged Foods') THEN 'Packaged Food'
         WHEN mc.master_category_names.level_one IN ('Meat / Seafood', 'Produce', 'Ready To Consume') THEN 'Ultra Fresh'
         ELSE '_unknown_'
     END AS level_zero,
     -- LEVEL_ONE calculation
     CASE
         WHEN mc.master_category_names.level_one IN ('', 'Bws', 'BWS') THEN 'BWS'
         WHEN mc.master_category_names.level_one IS NULL THEN '_unknown_'
         ELSE mc.master_category_names.level_one
     END AS level_one,
     -- LEVEL_TWO calculation
     CASE
         WHEN mc.master_category_names.level_two IS NULL OR mc.master_category_names.level_two = '' THEN '_unknown_'
         WHEN mc.master_category_names.level_two LIKE 'Apparel / Footwear%' THEN 'Apparel / Footwear / Sports Equipment'
         WHEN mc.master_category_names.level_two LIKE 'Frozen Fruit / Vegetables%' THEN 'Frozen Fruit / Vegetables / Potato'
         WHEN mc.master_category_names.level_two LIKE 'Prepared F&V%' THEN 'Prepared F&V / Fresh Herbs'
         ELSE mc.master_category_names.level_two
     END AS level_two,
     -- LEVEL_THREE calculation
     COALESCE(mc.master_category_names.level_three, '_unknown_') AS level_three,
     qcp.master_product_created_at_utc,
     qcp.chain_product_created_at_utc, 
    COALESCE(DATE(qcp.chain_product_created_at_utc), DATE('1999-01-01')) AS updated_at,
 FROM
     `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` AS qcp
 LEFT JOIN
     UNNEST(qcp.vendor_products) AS vp
 LEFT JOIN
     UNNEST(qcp.master_categories) AS mc
 WHERE
     vp.warehouse_id IS NOT NULL
     AND vp.warehouse_id != ''
     AND REGEXP_CONTAINS(qcp.country_code,  {{ params.param_country_code }})
 GROUP BY
     qcp.sku,
     qcp.pim_product_id,
     qcp.global_entity_id,
     qcp.country_code,
     vp.warehouse_id,
     qcp.brand_name,
     qcp.catalog_region,
     mc.master_category_names.level_one,
     mc.master_category_names.level_two,
     mc.master_category_names.level_three,
     qcp.master_product_created_at_utc,
     qcp.chain_product_created_at_utc
 QUALIFY
     ROW_NUMBER() OVER latest_chain = 1
 WINDOW
     latest_chain AS (
         PARTITION BY
             qcp.global_entity_id,
             qcp.country_code,
             qcp.sku,
             vp.warehouse_id
         ORDER BY
             qcp.chain_product_created_at_utc DESC NULLS LAST
     )
),
products AS (
SELECT qcp.*,
     -- 1. Try to get brand_owner_name via pim_product_id match (pp.brand_owner_name)
     -- 2. Fallback to get brand_owner_name via clean_brand_name match (pb.brand_owner_name_brand)
     COALESCE(pp.brand_owner_name, pb.brand_owner_name_brand) AS brand_owner_name,
     pp.brand_name_pim,
FROM qc_catalog_products AS qcp
 LEFT JOIN
     pim_products AS pp
     ON qcp.pim_product_id = pp.pim_product_id
 LEFT JOIN
     pim_brands AS pb
     ON qcp.clean_brand_name = pb.clean_brand_name
),
---
-- CTE : Maps Products to DC Warehouses (Used for Supplier Logic)
---
dc_warehouse_mappings AS (
 SELECT
     ps.global_entity_id,
     pr.country_code,
     pr.sku,
     pr.dc_warehouse_id,
     pr.warehouse_id
 FROM
     `{{ params.project_id }}.{{ params.dataset.cl }}.product_replenishment` AS pr
 INNER JOIN
     `{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
     ON pr.country_code = ps.country_code
     AND pr.sku = ps.sku
 WHERE
     pr.dc_warehouse_id IS NOT NULL
     AND REGEXP_CONTAINS(pr.country_code,  {{ params.param_country_code }})
 GROUP BY
     1, 2, 3, 4, 5
),
---
-- CTE : Combines Supplier-Product Data (Union for DC vs Non-DC Logic)
---
products_suppliers AS (
 -- LOGIC A: Products NOT MAPPED to a DC Warehouse
 SELECT DISTINCT
     ps.global_entity_id,
     ps.country_code,
     ps.sku,
     s.supplier_id,
     w.warehouse_id,
     '' AS dc_warehouse_id,
     w.is_preferred_supplier,
     DATE(s.supplier_updated_at) AS supplier_updated_at,
 FROM
     `{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
 CROSS JOIN
     UNNEST(ps.suppliers) AS s
 CROSS JOIN
     UNNEST(s.warehouses) AS w
 LEFT JOIN
     dc_warehouse_mappings AS dcm
     ON ps.country_code = dcm.country_code
     AND ps.sku = dcm.sku
     AND w.warehouse_id = dcm.dc_warehouse_id
 WHERE
     s.is_supplier_deleted = FALSE
     AND w.warehouse_id IS NOT NULL
     AND s.supplier_id IS NOT NULL
     AND dcm.warehouse_id IS NULL -- Exclude if mapped to DC
     AND REGEXP_CONTAINS(ps.country_code,  {{ params.param_country_code }})
  UNION ALL
  -- LOGIC B: Products MAPPED to a DC Warehouse
 SELECT DISTINCT
     dcm.global_entity_id,
     dcm.country_code,
     dcm.sku,
     s.supplier_id,
     dcm.warehouse_id,
     dcm.dc_warehouse_id,-- Use the DC warehouse ID
     w.is_preferred_supplier,
     DATE(s.supplier_updated_at) AS supplier_updated_at,
 FROM
     `{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
 CROSS JOIN
     UNNEST(ps.suppliers) AS s
 CROSS JOIN
     UNNEST(s.warehouses) AS w
 INNER JOIN
     dc_warehouse_mappings AS dcm
     ON ps.country_code = dcm.country_code
     AND ps.sku = dcm.sku
     AND w.warehouse_id = dcm.dc_warehouse_id -- Match on the DC warehouse ID
 WHERE
     s.is_supplier_deleted = FALSE
     AND w.warehouse_id IS NOT NULL
     AND s.supplier_id IS NOT NULL
     AND REGEXP_CONTAINS(ps.country_code,  {{ params.param_country_code }})
),
---
-- CTE : Ranks Suppliers based on Preference and Product Creation Date
---
supplier_products_ranked AS (
 SELECT
     ps.global_entity_id,
     ps.country_code,
     ps.sku,
     ps.supplier_id,
     ps.warehouse_id,
     ps.dc_warehouse_id,
     ps.is_preferred_supplier,
     qcp.master_product_created_at_utc,
     COALESCE(ps.supplier_updated_at, COALESCE(DATE(qcp.chain_product_created_at_utc), DATE('1999-01-01'))) AS supplier_updated_at,
     ROW_NUMBER() OVER (
         PARTITION BY
             ps.global_entity_id,
             ps.country_code,
             ps.sku,
             ps.warehouse_id
         ORDER BY
             ps.is_preferred_supplier DESC,
             qcp.master_product_created_at_utc DESC
     ) AS rn
 FROM
     products_suppliers AS ps
 LEFT JOIN
     products AS qcp
     ON ps.global_entity_id = qcp.global_entity_id
     AND ps.country_code = qcp.country_code
     AND ps.sku = qcp.sku
     AND ps.warehouse_id = qcp.warehouse_id
),
---
-- CTE : Selects the Top-Ranked Supplier for each Product/Warehouse
---
supplier_products AS (
 SELECT
     spr.global_entity_id,
     spr.country_code,
     spr.sku,
     spr.warehouse_id,
     spr.dc_warehouse_id,
     spr.supplier_updated_at AS updated_at,
     ANY_VALUE(spr.supplier_id) AS supplier_id,
 FROM
     supplier_products_ranked AS spr
 WHERE
     spr.rn = 1
 GROUP BY
     1, 2, 3, 4, 5, 6
),
---
-- CTE : Retrieves Supplier Hierarchy and Names from SRM
---
srm_suppliers AS (
 SELECT
     a.global_entity_id,
     a.country_code,
     a.supplier_name,
     a.global_supplier_id,
     a.supplier_id,
     a.ultimate_sup_id_parent AS sup_id_parent,
 FROM
     `{{ params.project_id }}.{{ params.dataset.cl }}.sps_supplier_hierarchy` AS a
     CROSS JOIN
 UNNEST(a.all_descendants_paired) AS adp
 WHERE
     TRUE
 GROUP BY 1, 2, 3, 4, 5, 6
),
---
-- CTE : Finds the latest non-null supplier for products without one
---
latest_non_null_supplier AS (
 SELECT
     t1.global_entity_id,
     t1.country_code,
     t1.sku,
     t1.supplier_id AS latest_supplier_id,
     ss.supplier_name AS latest_supplier_name,
     ss.global_supplier_id AS latest_global_supplier_id,
     ss.sup_id_parent AS latest_sup_id_parent,
 FROM
     supplier_products AS t1
 INNER JOIN
     srm_suppliers AS ss
     ON t1.global_entity_id = ss.global_entity_id
     AND CAST(t1.supplier_id AS STRING) = ss.supplier_id
 WHERE
     t1.supplier_id IS NOT NULL
 QUALIFY
     ROW_NUMBER() OVER (
         PARTITION BY
             t1.global_entity_id,
             t1.country_code,
             t1.sku
         ORDER BY
             t1.updated_at DESC
     ) = 1
),
---
-- CTE : FINAL ASSEMBLY with consistent Backfilling
---
sku_sup_warehouse_qc_catalog AS (
 WITH sku_backfill_map AS (
    -- This sub-CTE identifies the "Best" supplier for a SKU globally 
    -- to use when a specific warehouse mapping is missing.
    SELECT 
        global_entity_id,
        sku,
        supplier_id,
        sup_id_parent,
        global_supplier_id,
        supplier_name, 
        updated_at,
    FROM (
        SELECT 
            sp.global_entity_id,
            sp.sku,
            sp.supplier_id,
            ss.sup_id_parent,
            ss.global_supplier_id,
            ss.supplier_name,
            sp.updated_at,
            ROW_NUMBER() OVER (
                PARTITION BY sp.global_entity_id, sp.sku 
                ORDER BY sp.updated_at DESC, sp.supplier_id DESC
            ) as rank_priority
        FROM supplier_products AS sp
        INNER JOIN srm_suppliers AS ss
            ON sp.global_entity_id = ss.global_entity_id
            AND CAST(sp.supplier_id AS STRING) = ss.supplier_id
    )
    WHERE rank_priority = 1
 )
 SELECT DISTINCT
     qcp.region_code,
     qcp.global_entity_id,
     qcp.country_code,
     -- 1. Try specific warehouse supplier, else use the SKU-level best match
     COALESCE(CAST(sp.supplier_id AS STRING), CAST(bm.supplier_id AS STRING), '_unknown_') AS supplier_id,
     COALESCE(ss.sup_id_parent, bm.sup_id_parent, CAST(sp.supplier_id AS STRING), CAST(bm.supplier_id AS STRING)) AS sup_id_parent,
     COALESCE(ss.global_supplier_id, bm.global_supplier_id, '_unknown_') AS global_supplier_id,
     COALESCE(ss.supplier_name, bm.supplier_name, '_unknown_') AS supplier_name,
     coalesce(qcp.updated_at, sp.updated_at) AS updated_at,
     qcp.sku,
     COALESCE(qcp.brand_name, '_unknown_') AS brand_name,
     COALESCE(qcp.brand_owner_name, '_unknown_') AS brand_owner_name,
     qcp.level_zero,
     qcp.level_one,
     qcp.level_two,
     qcp.level_three,
     qcp.warehouse_id,
     sp.dc_warehouse_id
 FROM
     products AS qcp
 LEFT JOIN
     supplier_products AS sp
     ON qcp.global_entity_id = sp.global_entity_id
     AND qcp.sku = sp.sku
     AND qcp.warehouse_id = sp.warehouse_id
 LEFT JOIN
     srm_suppliers AS ss
     ON sp.global_entity_id = ss.global_entity_id
     AND CAST(sp.supplier_id AS STRING) = ss.supplier_id
 -- Join the backfill map
 LEFT JOIN
     sku_backfill_map AS bm
     ON qcp.global_entity_id = bm.global_entity_id
     AND qcp.sku = bm.sku
),
----------------------------------------------------------------------- 2 PURCHASE ORDERS (DYNAMIC DATE FILTERS APPLIED) ------------------------------------------------------------------------
-- CTE : Base for Purchase Order Data (Combines unnesting, filtering, and aggregation)
purchase_orders AS (
 SELECT
   po.global_entity_id,
   po.country_code,
   pp.sku_id,
   po.supplier_id,
   po.supplier_name,
   po.warehouse_id,
   po.updated_at,
   pp.created_localtime_at AS po_creation_timestamp,
   SUM(receiving.received_qty) AS qty_received
 FROM
   `{{ params.project_id }}.{{ params.dataset.cl }}.purchase_orders` AS po
 LEFT JOIN
   UNNEST(po.products_purchased) AS pp
 LEFT JOIN
   UNNEST(pp.receiving) AS receiving
 WHERE
   -- DYNAMIC 24-MONTH FILTER APPLIED HERE:
    DATE(po.fulfilled_localtime_at) BETWEEN date_start_current AND run_anchor_date
 GROUP BY
   1, 2, 3, 4, 5, 6, 7, 8
),
--- CTE : supplied through a DC (Centralized)------------------
distribution_centers AS (
 SELECT w.warehouse_id
 FROM
 `{{ params.project_id }}.{{ params.dataset.cl }}.warehouses_v2` AS w
 LEFT JOIN UNNEST(w.vendors) AS vendors
 WHERE w.is_distribution_center = TRUE AND vendors.migrated_at_utc IS NULL
 GROUP BY 1
),
store_transfers_base AS (
 SELECT
   t.global_entity_id,
   t.country_code,
   t.dest_warehouse_id AS destination_warehouse_id,
   t.src_warehouse_id AS source_warehouse_id,
   products.sku AS sku_id, -- Unnested SKU ID
   status_history.modified_at AS status_modified_at, -- Unnested modified timestamp
   status_history.status AS transfer_status -- Unnested transfer status
 FROM
   `{{ params.project_id }}.{{ params.dataset.cl }}.store_transfers` AS t
 LEFT JOIN
   UNNEST(t.status_history) AS status_history
 LEFT JOIN
   UNNEST(t.products) AS products
 -- Applying the completed status filter here for early efficiency
 WHERE
   status_history.status = 'COMPLETED'
   -- DYNAMIC 24-MONTH FILTER APPLIED HERE:
   AND DATE(status_history.modified_at) BETWEEN date_start_current AND run_anchor_date
),
--- CTE : sku and warehouse transfers from DC (Centralized)------------------
sku_wh_centralized AS (
 SELECT DISTINCT
     st.sku_id,
     st.global_entity_id,
     st.country_code,
     st.destination_warehouse_id,
     st.source_warehouse_id,
     ROW_NUMBER() OVER (PARTITION BY st.sku_id, st.destination_warehouse_id ORDER BY st.status_modified_at DESC NULLS LAST) AS ranking
 FROM
 store_transfers_base AS st
 WHERE st.source_warehouse_id IN (SELECT warehouse_id FROM distribution_centers)
 AND st.transfer_status = "COMPLETED"
),
-- CASE 1!! Select the best supplier for the centralized SKUs (Merges the ranking and filtering steps)
sku_supplier_dc AS (
 SELECT
     po.global_entity_id,
     po.country_code,
     po.sku_id,
     po.supplier_id,
     po.supplier_name,
     t.destination_warehouse_id AS warehouse_id,
     DATE(po.po_creation_timestamp) AS updated_at,
 FROM
   purchase_orders AS po
 INNER JOIN -- Filter POs to only those made to a DC
     distribution_centers AS dc
     ON po.warehouse_id = dc.warehouse_id
 INNER JOIN
     sku_wh_centralized AS t
     ON t.sku_id = po.sku_id
     AND t.global_entity_id = po.global_entity_id
 WHERE
     po.supplier_name NOT IN ("Pista Falsa", "CD", "Repartos Ya SA")
     AND t.ranking = 1 -- Only consider the latest completed transfer
 -- Use QUALIFY to immediately filter for the top-ranked Purchase Order (PO)
 -- based on the creation date per SKU/Destination WH combination.
 QUALIFY
     RANK() OVER (
         PARTITION BY po.sku_id, t.destination_warehouse_id
         ORDER BY po.po_creation_timestamp DESC NULLS LAST
     ) = 1
),
-- CASE 2!! Case for direct delivery
supplier_direct_delivery AS (
 SELECT
     po.global_entity_id,
     po.country_code,
     po.sku_id,
     po.supplier_id,
     po.supplier_name,
     po.warehouse_id,
     DATE(po.po_creation_timestamp) AS updated_at,
 FROM
   purchase_orders AS po
 WHERE
     po.supplier_name NOT IN ("Pista Falsa", "CD", "Repartos Ya SA")
 --  MERGED STEP: Calculate rank and immediately filter to keep only the best record (Rank = 1)
 QUALIFY
     RANK() OVER (
         PARTITION BY po.warehouse_id, po.supplier_id
         ORDER BY po.po_creation_timestamp DESC NULLS LAST
     ) = 1
),
-- CASE 3: Find all completed store transfers *excluding* those sourced from a Distribution Center (DC).
store_to_store AS (
 WITH source_not_cd AS (
     SELECT
         st.global_entity_id,
         st.country_code,
         st.sku_id,
         st.destination_warehouse_id,
         st.source_warehouse_id,
         st.status_modified_at,
         -- Calculate ranking here to filter on the latest record immediately
         ROW_NUMBER() OVER (
             PARTITION BY st.sku_id, st.destination_warehouse_id
             ORDER BY st.status_modified_at DESC NULLS LAST
         ) AS ranking
     FROM
       store_transfers_base AS st
     -- Use LEFT JOIN/WHERE NULL (Anti-Join) to exclude DC sources
     LEFT JOIN
         distribution_centers AS dc
         ON st.source_warehouse_id = dc.warehouse_id
     WHERE
         st.transfer_status = "COMPLETED"
         AND dc.warehouse_id IS NULL -- Only keep transfers NOT sourced from a DC
 )
 SELECT DISTINCT
     snc.global_entity_id,
     snc.country_code,
     snc.sku_id,
     -- Merge supplier data (CD > Direct)
     CAST(COALESCE(sku_supplier_dc.supplier_id, supplier_direct_delivery.supplier_id) AS STRING) AS supplier_id,
     CAST(COALESCE(sku_supplier_dc.supplier_name, supplier_direct_delivery.supplier_name) AS STRING) AS supplier_name,
     snc.destination_warehouse_id AS warehouse_id,
     CAST(COALESCE(sku_supplier_dc.updated_at,supplier_direct_delivery.updated_at, DATE(snc.status_modified_at)) AS DATE ) AS updated_at,
 FROM
     source_not_cd AS snc
 LEFT JOIN
     sku_supplier_dc
     ON snc.sku_id = sku_supplier_dc.sku_id
     AND snc.source_warehouse_id = sku_supplier_dc.warehouse_id
 LEFT JOIN
     supplier_direct_delivery
     ON snc.sku_id = supplier_direct_delivery.sku_id
     AND snc.source_warehouse_id = supplier_direct_delivery.warehouse_id
 -- Filter on the latest transfer record (ranking = 1)
 WHERE
     snc.ranking = 1
),
-- CASE 4: The below query is to get the supplier_id of the SKUS that might not have a supplier_id and sku combination in the PO table but have it in the CO table
exclusion_warehouses AS (
 -- Combine DCs (from warehouses_v2) and PC/DCs (from gsheet_rdvr) into one list
  SELECT
     dc.warehouse_id
 FROM
     distribution_centers AS dc
 UNION DISTINCT
 SELECT
     warehouse_id
 FROM
     `{{ params.project_id }}.{{ params.dataset.dl }}.gsheet_rdvr_scm_centralization_DC_PC_list`
),
missing_supplier_skus AS (
 SELECT
   po.global_entity_id,
   po.country_code,
   po.sku_id,       
   COALESCE(CAST(ls.latest_supplier_id AS STRING), po.supplier_id) AS supplier_id,
   COALESCE(ls.latest_supplier_name, po.supplier_name) AS supplier_name,
   COUNT(DISTINCT po.warehouse_id) AS nr_warehouses,
   SUM(po.qty_received) AS qty_received_total, 
   DATE(po.updated_at) AS updated_at,
 FROM
   purchase_orders AS po
 LEFT JOIN
   exclusion_warehouses AS exw
   ON po.warehouse_id = exw.warehouse_id
 LEFT JOIN
     latest_non_null_supplier AS ls
     ON po.global_entity_id = ls.global_entity_id
     AND po.sku_id = ls.sku
 WHERE
   exw.warehouse_id IS NULL -- Anti-Join: Only select warehouses NOT in the exclusion list
   AND po.supplier_name NOT IN ('Pista Falsa', 'CD', 'Repartos Ya SA')
 GROUP BY
   1, 2, 3, 4, 5, po.updated_at 
 QUALIFY
   RANK() OVER (
     PARTITION BY po.sku_id, supplier_id, po.global_entity_id , po.country_code
     ORDER BY
       COUNT(DISTINCT po.warehouse_id) DESC,
       SUM(po.qty_received) DESC,
       po.updated_at DESC
   ) = 1
),
sku_sup_warehouse_purch_ord AS(
SELECT
 COALESCE(sdd.global_entity_id, sts.global_entity_id, sdc.global_entity_id, mss.global_entity_id) AS global_entity_id,
 COALESCE(sdd.sku_id, sts.sku_id, sdc.sku_id, mss.sku_id) AS sku_id,
 COALESCE(sdd.warehouse_id, sts.warehouse_id, sdc.warehouse_id) AS warehouse_id,
 COALESCE(sdd.supplier_name, sts.supplier_name, sdc.supplier_name, mss.supplier_name) AS supplier_name,
 COALESCE(
     CAST(sdd.supplier_id AS STRING),
     CAST(sts.supplier_id AS STRING),
     CAST(sdc.supplier_id AS STRING),
     CAST(mss.supplier_id AS STRING)
 ) AS supplier_id,
 COALESCE(
     CASE WHEN sdd.supplier_id IS NOT NULL THEN 'direct_delivery' END,
     CASE WHEN sts.supplier_id IS NOT NULL THEN 'store_to_store' END,
     CASE WHEN sdc.supplier_id IS NOT NULL THEN 'distr_center' END,
     CASE WHEN mss.supplier_id IS NOT NULL THEN 'missing_supplier' END
 ) AS mapping_type, 
 COALESCE(sdd.updated_at, sts.updated_at, sdc.updated_at, mss.updated_at) AS updated_at,
FROM
 supplier_direct_delivery AS sdd
FULL OUTER JOIN
 store_to_store AS sts
 ON sdd.sku_id = sts.sku_id
 AND sdd.warehouse_id = sts.warehouse_id
 AND sdd.global_entity_id = sts.global_entity_id
FULL OUTER JOIN
 sku_supplier_dc AS sdc
 ON COALESCE(sdd.sku_id, sts.sku_id) = sdc.sku_id
 AND COALESCE(sdd.warehouse_id, sts.warehouse_id) = sdc.warehouse_id
 AND COALESCE(sdd.global_entity_id, sts.global_entity_id) = sdc.global_entity_id
FULL OUTER JOIN
 missing_supplier_skus AS mss
 ON COALESCE(sdd.sku_id, sts.sku_id, sdc.sku_id) = mss.sku_id
 AND COALESCE(sdd.global_entity_id, sts.global_entity_id) = mss.global_entity_id
),
----------------------------------------------------------------------- 3 FINAL SKU SUP WAREHOUSE MAPPING ------------------------------------------------------------------------
sku_sup_warehouse_qc_catalog_agg_1 AS (
 SELECT
     global_entity_id,
     sku,
     warehouse_id,
     CAST(supplier_id AS STRING) AS supplier_id,
     supplier_name, 
     DATE(updated_at) AS updated_at,
     -- All other QC metadata fields are excluded here as per final requirement
 FROM
     sku_sup_warehouse_qc_catalog
 GROUP BY 1,2,3,4,5,6
),
sup_qc_catalog_agg_1 AS (
 SELECT
     global_entity_id,
     CAST(supplier_id AS STRING) AS supplier_id,
     supplier_name, 
    --  sup_id_parent,
 FROM
     sku_sup_warehouse_qc_catalog
 GROUP BY 1,2,3
),
sku_sup_warehouse AS (
--FINAL CONSOLIDATION
SELECT DISTINCT
 -- Output Columns matching sku_sup_warehouse_purch_ord structure:
 COALESCE(po.global_entity_id, qc.global_entity_id) AS global_entity_id,
 COALESCE(po.sku_id, qc.sku) AS sku_id,
 COALESCE(po.warehouse_id, qc.warehouse_id) AS warehouse_id,
 -- Supplier Fields (Prioritize PO Mapping, Fallback to QC Catalog)
 COALESCE(po.supplier_id, qc.supplier_id, '_unknown_') AS supplier_id,
 COALESCE(po.supplier_name,ca.supplier_name, qc.supplier_name, '_unknown_') AS supplier_name,
 -- Mapping Type (Prioritize PO Mapping, Fallback to QC Catalog)
 COALESCE(po.mapping_type, 'qc_catalog') AS mapping_type, 
 COALESCE(po.updated_at, qc.updated_at) AS updated_at,
FROM
 (sku_sup_warehouse_purch_ord AS po
    LEFT JOIN sup_qc_catalog_agg_1 ca
    ON po.global_entity_id = ca.global_entity_id
        AND po.supplier_id = ca.supplier_id)
FULL OUTER JOIN
 sku_sup_warehouse_qc_catalog_agg_1 AS qc
 ON po.global_entity_id = qc.global_entity_id
 AND po.sku_id = qc.sku
 AND po.warehouse_id = qc.warehouse_id
),
sources AS (
   SELECT
     country_code,
     global_entity_id
 FROM
   `{{ params.project_id }}.{{ params.dataset.cl }}.sources`
 GROUP BY 1,2
),
sku_sup_qc_catalog_agg AS (
 SELECT
     sqc.global_entity_id,
     sqc.sku,
     COALESCE(CAST(sqc.supplier_id AS STRING), '_unknown_') AS supplier_id,
     ss.sup_id_parent,
     ss.global_supplier_id,
     sqc.brand_name,
     sqc.brand_owner_name,
     sqc.level_zero,
     sqc.level_one,
     sqc.level_two,
     sqc.level_three,
     sqc.region_code,
 FROM
     sku_sup_warehouse_qc_catalog AS sqc
  LEFT JOIN srm_suppliers AS ss
   ON sqc.global_entity_id = ss.global_entity_id
    AND sqc.supplier_id = ss.supplier_id
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),
joined_data AS (
    SELECT 
        ssw.global_entity_id,
        s.country_code,
        ssw.sku_id,
        ssw.warehouse_id,
        ssw.supplier_id,
        ssw.supplier_name, 
        ssw.mapping_type,
        ssw.updated_at,
        COALESCE(
            ss.sup_id_parent, 
            FIRST_VALUE(qc.sup_id_parent IGNORE NULLS) OVER(PARTITION BY ssw.supplier_id ORDER BY ssw.updated_at DESC), 
            '_unknown_'
        ) AS sup_id_parent, 
        COALESCE(
            ss.global_supplier_id, 
            FIRST_VALUE(qc.global_supplier_id IGNORE NULLS) OVER(PARTITION BY ssw.supplier_id ORDER BY ssw.updated_at DESC), 
            '_unknown_'
        ) AS global_supplier_id,
        MAX(qc.brand_name) OVER(PARTITION BY ssw.sku_id) AS brand_name,
        MAX(qc.brand_owner_name) OVER(PARTITION BY ssw.sku_id) AS brand_owner_name,
        MAX(qc.level_zero) OVER(PARTITION BY ssw.sku_id) AS level_zero,
        MAX(qc.level_one) OVER(PARTITION BY ssw.sku_id) AS level_one,
        MAX(qc.level_two) OVER(PARTITION BY ssw.sku_id) AS level_two,
        MAX(qc.level_three) OVER(PARTITION BY ssw.sku_id) AS level_three,
        MAX(qc.region_code) OVER(PARTITION BY ssw.sku_id) AS region_code,
    FROM sku_sup_warehouse AS ssw
    LEFT JOIN srm_suppliers AS ss
    ON ssw.global_entity_id = ss.global_entity_id
        AND ssw.supplier_id = ss.supplier_id
    LEFT JOIN sku_sup_qc_catalog_agg AS qc
    ON ssw.global_entity_id = qc.global_entity_id
    AND ssw.sku_id = qc.sku
    AND ssw.supplier_id = qc.supplier_id
    LEFT JOIN sources AS s
    ON ssw.global_entity_id = s.global_entity_id
    WHERE TRUE
    AND REGEXP_CONTAINS(ssw.global_entity_id, {{ params.param_global_entity_id }})
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY ssw.global_entity_id, ssw.warehouse_id, ssw.sku_id 
        ORDER BY ssw.updated_at DESC
    ) = 1
), 
parent_list AS (
    SELECT sup_id_parent, global_entity_id
    FROM joined_data 
    WHERE sup_id_parent != '_unknown_'
    AND sup_id_parent != supplier_id 
    GROUP BY 1,2
)
-- Final Output
SELECT 
    jd.*,
    -- Label Division based on the calculated sup_id_parent from the join
    CASE 
        WHEN pl.sup_id_parent IS NULL THEN 'Division' 
        ELSE NULL 
    END AS division_type,
    -- Label Parent based on the final list
    IF(pl.sup_id_parent IS NOT NULL, TRUE, FALSE) AS is_parent_supplier
FROM joined_data jd
LEFT JOIN parent_list pl 
    ON jd.global_entity_id = pl.global_entity_id
    AND jd.supplier_id = pl.sup_id_parent
