

-- full refresh ---




-- VARIABLE DECLARATION
DECLARE param_country_code STRING DEFAULT "eg|cl|sg|th|hu|es|jo|kw|ar|ae|qa|pe|tr|ua|it|om|bh|hk|ph|sa";
-- DECLARE param_country_code STRING DEFAULT "ae";

 


CREATE OR REPLACE TABLE `dh-darkstores-live.christian_larosa.sps_product`
CLUSTER BY
   global_entity_id,
   warehouse_id,
   supplier_id,
   sku_id
AS
----------------------------------------------------------------------- 1 QC CATALOG (final CTE sku_sup_warehouse_qc_catalog) ------------------------------------------------------------------------
---
-- CTE 1: Extracts and Categorizes Product Data from qc_catalog_products
---
WITH qc_catalog_products AS (
   SELECT
       qcp.sku,
       MAX(qcp.product_name) AS product_name,
       qcp.pim_product_id,
       qcp.global_entity_id,
       qcp.country_code,
       vp.warehouse_id,
       qcp.brand_name,
      
       -- LEVEL_ZERO calculation
       CASE
           WHEN LOWER(mc.master_category_names.level_one) IN ('bws') THEN 'BWS'
           WHEN mc.master_category_names.level_one IN ('Bread / Bakery', 'Dairy / Chilled / Eggs') THEN 'Fresh'
           WHEN mc.master_category_names.level_one IN ('General Merchandise') THEN 'General Merchandise'
           WHEN mc.master_category_names.level_one IN ('Beverages', 'Snacks') THEN 'Impulse'
           WHEN mc.master_category_names.level_one IN ('Home / Pet', 'Personal Care / Baby / Health', 'Smoking / Tobacco') THEN 'Non-Food Grocery'
           WHEN mc.master_category_names.level_one IN ('Frozen', 'Packaged Foods') THEN 'Packaged Food'
           WHEN mc.master_category_names.level_one IN ('Meat / Seafood', 'Produce', 'Ready To Consume') THEN 'Ultra Fresh'
           ELSE 'Unknown'
       END AS level_zero,
      
       -- LEVEL_ONE calculation
       CASE
           WHEN mc.master_category_names.level_one IN ('', 'Bws', 'BWS') THEN 'BWS'
           WHEN mc.master_category_names.level_one IS NULL THEN 'Unknown'
           ELSE mc.master_category_names.level_one
       END AS level_one,
      
       -- LEVEL_TWO calculation
       CASE
           WHEN mc.master_category_names.level_two IS NULL OR mc.master_category_names.level_two = '' THEN 'Unknown'
           WHEN mc.master_category_names.level_two LIKE 'Apparel / Footwear%' THEN 'Apparel / Footwear / Sports Equipment'
           WHEN mc.master_category_names.level_two LIKE 'Frozen Fruit / Vegetables%' THEN 'Frozen Fruit / Vegetables / Potato'
           WHEN mc.master_category_names.level_two LIKE 'Prepared F&V%' THEN 'Prepared F&V / Fresh Herbs'
           ELSE mc.master_category_names.level_two
       END AS level_two,
      
       COALESCE(mc.master_category_names.level_three, 'Unknown') AS level_three,
       qcp.master_product_created_at_utc,
       qcp.chain_product_created_at_utc
      
   FROM
       --`{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` AS qcp
       `fulfillment-dwh-production.cl_dmart.qc_catalog_products` AS qcp
   LEFT JOIN
       UNNEST(qcp.vendor_products) AS vp
   LEFT JOIN
       UNNEST(qcp.master_categories) AS mc
   WHERE
       vp.warehouse_id IS NOT NULL
       AND vp.warehouse_id != ''
       --AND REGEXP_CONTAINS(qcp.country_code, param_country_code)
   GROUP BY
       qcp.sku,
       qcp.pim_product_id,
       qcp.global_entity_id,
       qcp.country_code,
       vp.warehouse_id,
       qcp.brand_name,
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


---
-- CTE 2: Maps Products to DC Warehouses (Used for Supplier Logic)
---
dc_warehouse_mappings AS (
   SELECT
       ps.global_entity_id,
       pr.country_code,
       pr.sku,
       pr.dc_warehouse_id,
       pr.warehouse_id
   FROM
       --`{{ params.project_id }}.{{ params.dataset.cl }}.product_replenishment` AS pr
       `fulfillment-dwh-production.cl_dmart.product_replenishment` AS pr
   INNER JOIN
       --`{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
       `fulfillment-dwh-production.cl_dmart.products_suppliers` AS ps
       ON pr.country_code = ps.country_code
       AND pr.sku = ps.sku
   WHERE
       pr.dc_warehouse_id IS NOT NULL
       ---AND REGEXP_CONTAINS(pr.country_code, param_country_code)
   GROUP BY
       1, 2, 3, 4, 5
),


---
-- CTE 3: Combines Supplier-Product Data (Union for DC vs Non-DC Logic)
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
       s.supplier_updated_at,
   FROM
       --`{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
       `fulfillment-dwh-production.cl_dmart.products_suppliers` AS ps
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
       ---AND REGEXP_CONTAINS(ps.country_code, param_country_code)
  
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
       s.supplier_updated_at,
   FROM
       --`{{ params.project_id }}.{{ params.dataset.cl }}.products_suppliers` AS ps
       `fulfillment-dwh-production.cl_dmart.products_suppliers` AS ps
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
       ---AND REGEXP_CONTAINS(ps.country_code, param_country_code)
),


---
-- CTE 4: Ranks Suppliers based on Preference and Product Creation Date
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
       ps.supplier_updated_at,
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
       qc_catalog_products AS qcp
       ON ps.global_entity_id = qcp.global_entity_id
       AND ps.country_code = qcp.country_code
       AND ps.sku = qcp.sku
       AND ps.warehouse_id = qcp.warehouse_id
),


---
-- CTE 5: Selects the Top-Ranked Supplier for each Product/Warehouse
---
supplier_products AS (
   SELECT
       spr.global_entity_id,
       spr.country_code,
       spr.sku,
       spr.warehouse_id,
       spr.dc_warehouse_id,
       spr.supplier_updated_at,
       ANY_VALUE(spr.supplier_id) AS supplier_id,
   FROM
       supplier_products_ranked AS spr
   WHERE
       spr.rn = 1
   GROUP BY
       1, 2, 3, 4, 5, 6
),


---
-- CTE 6: Retrieves Supplier Hierarchy and Names from SRM
---
srm_suppliers AS (
   SELECT
       a.global_entity_id,
       a.country_code,
       a.supplier_name,
       a.global_supplier_id,
       a.supplier_id,
       a.ultimate_sup_id_parent AS sup_id_parent,
       -- a.is_ultimate_sup_id_parent AS is_sup_id_parent,
       STRING_AGG(adp.supplier_id, ', ') AS all_descendant_supplier_ids_string
   FROM
       `dh-darkstores-live.christian_larosa.sps_supplier_hierarchy` AS a
       CROSS JOIN
   UNNEST(a.all_descendants_paired) AS adp
   WHERE
       TRUE
   -- and a.supplier_id = '206'
   -- and a.global_entity_id = 'TB_AE'
   GROUP BY 1, 2, 3, 4, 5, 6
),


---
-- CTE 7: Maps Region Codes to Names
---
region_mapping AS (
   SELECT
       rm.pim_region_code AS region_code,
       rm.pim_region_name AS region_name,
       rm.country_code
   FROM
       `dh-darkstores-live.cl_data_science_qcommerce.pim_region_entity_mapping` AS rm
   WHERE
       TRUE
),


---
-- CTE 8: Selects the Latest Product Info from PIM
---
pim_products AS (
   SELECT
       pp.region_code,
       rm.region_name,
       pp.product_id AS pim_product_id,
       pp.brand_owner_name,
       pp.brands_name AS brand_name,
       rm.country_code
   FROM
       --`{{ params.project_id }}.{{ params.dataset.curated_data_shared }}.pim_product`AS p
       `fulfillment-dwh-production.cl_dmart.pim_product` AS pp
   LEFT JOIN
       UNNEST(pp.brand_local_names) AS bln
   LEFT JOIN
       region_mapping AS rm
       ON pp.region_code = rm.region_code
   WHERE
       TRUE
   QUALIFY
       ROW_NUMBER() OVER (
           PARTITION BY pp.region_code, pp.product_id
           ORDER BY
               pp.product_updated_at DESC,
               (pp.brand_uuid IS NOT NULL) DESC
       ) = 1
),


---
-- CTE 9: Finds the latest non-null supplier for products without one
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
       ss.all_descendant_supplier_ids_string,
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
               t1.supplier_updated_at DESC
       ) = 1
),


---
-- CTE 10: FINAL ASSEMBLY of all product and supplier details
---
sku_sup_warehouse_qc_catalog AS (
   SELECT DISTINCT
       rm.region_code,
       rm.region_name,
       qcp.global_entity_id,
       qcp.country_code,
       -- Backfilling supplier_id with latest non-null supplier if current is NULL
       COALESCE(sp.supplier_id, latest_sup.latest_supplier_id) AS supplier_id,
       -- Backfilling sup_id_parent with latest non-null supplier parent ID if current is NULL
       COALESCE(ss.sup_id_parent, latest_sup.latest_sup_id_parent) AS sup_id_parent,
       -- ss.is_sup_id_parent,
       -- Backfilling global_supplier_id with latest non-null supplier global ID if current is NULL
       COALESCE(ss.global_supplier_id, latest_sup.latest_global_supplier_id) AS global_supplier_id,
       -- Backfilling supplier_name with latest non-null supplier name if current is NULL
       COALESCE(ss.supplier_name, latest_sup.latest_supplier_name) AS supplier_name,
       COALESCE(ss.all_descendant_supplier_ids_string, latest_sup.all_descendant_supplier_ids_string) AS all_descendant_supplier_ids_string,
       sp.supplier_updated_at,
       qcp.sku,
              COALESCE(qcp.product_name, 'Unknown') AS product_name,
       COALESCE(qcp.brand_name, pp.brand_name, 'Unknown') AS brand_name,
       COALESCE(pp.brand_owner_name, 'Unknown') AS brand_owner_name,
       qcp.level_zero,
       qcp.level_one,
       qcp.level_two,
       qcp.level_three,
       qcp.warehouse_id,
       sp.dc_warehouse_id,
   FROM
       qc_catalog_products AS qcp
   LEFT JOIN
       supplier_products AS sp
       ON qcp.global_entity_id = sp.global_entity_id
       AND qcp.sku = sp.sku
       AND qcp.warehouse_id = sp.warehouse_id
   LEFT JOIN
       srm_suppliers AS ss
       ON sp.global_entity_id = ss.global_entity_id
       AND CAST(sp.supplier_id AS STRING) = ss.supplier_id
   LEFT JOIN
       pim_products AS pp
       ON qcp.country_code = pp.country_code
       AND qcp.pim_product_id = pp.pim_product_id
   LEFT JOIN
       region_mapping AS rm
       ON qcp.country_code = rm.country_code
   LEFT JOIN
       latest_non_null_supplier AS latest_sup
       ON qcp.global_entity_id = latest_sup.global_entity_id
       AND qcp.sku = latest_sup.sku
),


-- SELECT
--     * FROM sku_sup_warehouse_qc_catalog


--     where sku = '905551'
   --and warehouse_id = '7ab4052a-33e4-4e52-9d47-844b61753c6c'
-- FINAL SELECT STATEMENT
-- SELECT
--     * FROM
--     `dh-central-salesforce-tech.salesforce_platform_analytics_staging.sps_product`
-- WHERE
--     TRUE
-- AND sku = '925665'


----------------------------------------------------------------------- 2 PURCHASE ORDERS (final CTE sku_sup_warehouse_purch_ord) ------------------------------------------------------------------------




 -- CTE: Base  for Purchase Order Data (Combines unnesting, filtering, and aggregation)
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
    --`{{ params.project_id }}.{{ params.dataset.cl }}.purchase_orders` AS po
     `fulfillment-dwh-production.cl_dmart.purchase_orders` AS po
   LEFT JOIN
     UNNEST(po.products_purchased) AS pp
   LEFT JOIN
     UNNEST(pp.receiving) AS receiving
   WHERE
     DATE_TRUNC(DATETIME(po.fulfilled_localtime_at), MONTH) BETWEEN '2023-10-01' AND '2025-10-31'
     --AND REGEXP_CONTAINS(po.country_code, 'ae')
   GROUP BY
     1, 2, 3, 4, 5, 6, 7, 8
 ),


--- CTE: supplied through a DC------------------
distribution_centers AS (
   SELECT w.warehouse_id
   FROM
   -- `{{ params.project_id }}.{{ params.dataset.cl }}.warehouses_v2` AS w
   `fulfillment-dwh-production.cl_dmart.warehouses_v2` AS w
   LEFT JOIN UNNEST(w.vendors) AS vendors
   WHERE w.is_distribution_center = TRUE AND vendors.migrated_at_utc IS NULL
   GROUP BY 1
)
,
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
     `fulfillment-dwh-production.cl_dmart.store_transfers` AS t
   LEFT JOIN
     UNNEST(t.status_history) AS status_history
   LEFT JOIN
     UNNEST(t.products) AS products
   -- Applying the completed status filter here for early efficiency
   WHERE
     status_history.status = 'COMPLETED'
     --AND REGEXP_CONTAINS(t.global_entity_id, 'TB_AE')
     AND DATE_TRUNC(DATETIME(status_history.modified_at), MONTH) BETWEEN '2023-10-01' AND '2025-10-31'  
 ),
--- CTE: sku and warehouse transfers from DC (Centralized)------------------
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
)
,
-- CASE 1!! Select the best supplier for the centralized SKUs (Merges the ranking and filtering steps)
sku_supplier_dc AS (
   SELECT
       po.global_entity_id,
       po.country_code,
       po.sku_id,
       po.supplier_id,
       po.supplier_name,
       t.destination_warehouse_id AS warehouse_id,
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
)
,
-- CASE 2!! Case for direct delivery
supplier_direct_delivery AS (
   SELECT
       po.global_entity_id,
       po.country_code,
       po.sku_id,
       po.supplier_id,
       po.supplier_name,
       po.warehouse_id,
   FROM
     purchase_orders AS po
     -- LEFT JOIN  UNNEST(po.products_purchased) AS pp
   WHERE
       po.supplier_name NOT IN ("Pista Falsa", "CD", "Repartos Ya SA")
   --  MERGED STEP: Calculate rank and immediately filter to keep only the best record (Rank = 1)
   QUALIFY
       RANK() OVER (
           PARTITION BY po.warehouse_id, po.supplier_id
           ORDER BY po.po_creation_timestamp DESC NULLS LAST
       ) = 1
)
,
-- CASE 3: Find all completed store transfers *excluding* those sourced from a Distribution Center (DC).
store_to_store AS (
   -- This uses an Anti-Join (LEFT JOIN ... WHERE NULL) for efficiency.
   WITH source_not_cd AS (
       SELECT
           st.global_entity_id,
           st.country_code,
           st.sku_id,
           st.destination_warehouse_id,
           st.source_warehouse_id,
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
       CAST(COALESCE(sku_supplier_dc.supplier_id, supplier_direct_delivery.supplier_id) AS INT64) AS supplier_id,
       CAST(COALESCE(sku_supplier_dc.supplier_name, supplier_direct_delivery.supplier_name) AS STRING) AS supplier_name,
       snc.destination_warehouse_id AS warehouse_id,
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
       `fulfillment-dwh-production.dl_dmart.gsheet_rdvr_scm_centralization_DC_PC_list`
),
missing_supplier_skus AS (
   SELECT
     po.global_entity_id,
     po.country_code,
     po.sku_id,         
     COALESCE(CAST(ls.latest_supplier_id AS STRING), po.supplier_id) AS supplier_id, -- Column 3
     COALESCE(ls.latest_supplier_name, po.supplier_name) AS supplier_name,         -- Column 4
     COUNT(DISTINCT po.warehouse_id) AS nr_warehouses,
     SUM(po.qty_received) AS qty_received_total
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
     1, 2, 3, 4, 5, po.updated_at -- Note: supplier_id is Grouping Key (Column 3)
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
   ) AS mapping_type
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
)
,






----------------------------------------------------------------------- 3 FINAL SKU SUP WAREHOUSE MAPPING (final CTE sku_sup_warehouse_purch_ord) ------------------------------------------------------------------------
sku_sup_warehouse_qc_catalog_agg_1 AS (
   SELECT
       global_entity_id,
       sku,
       warehouse_id,
       CAST(supplier_id AS STRING) AS supplier_id, -- Ensure supplier_id type matches PO logic
       supplier_name
       -- All other QC metadata fields are excluded here as per final requirement
   FROM
       sku_sup_warehouse_qc_catalog
   GROUP BY 1,2,3,4,5
),


sku_sup_warehouse AS (
-- FINAL CONSOLIDATION
SELECT DISTINCT
   -- Output Columns matching sku_sup_warehouse_purch_ord structure:
   COALESCE(po.global_entity_id, qc.global_entity_id) AS global_entity_id,
   COALESCE(po.sku_id, qc.sku) AS sku_id,
   COALESCE(po.warehouse_id, qc.warehouse_id) AS warehouse_id,


   -- Supplier Fields (Prioritize PO Mapping, Fallback to QC Catalog)
   COALESCE(po.supplier_id, qc.supplier_id) AS supplier_id,
   COALESCE(po.supplier_name, qc.supplier_name) AS supplier_name,


   -- Mapping Type (Prioritize PO Mapping, Fallback to QC Catalog)
   COALESCE(po.mapping_type, 'qc_catalog') AS mapping_type


FROM
   sku_sup_warehouse_purch_ord AS po
FULL OUTER JOIN
   sku_sup_warehouse_qc_catalog_agg_1 AS qc
   -- Join on the core product/entity keys
   ON po.global_entity_id = qc.global_entity_id
   AND po.sku_id = qc.sku
   AND po.warehouse_id = qc.warehouse_id
),


-- SELECT * FROM sku_sup_warehouse
-- where sku_id = '500006'
sources AS (
     SELECT
       country_code,
       global_entity_id
   FROM
     `fulfillment-dwh-production.cl_dmart.sources`
   GROUP BY 1,2
),


sku_sup_qc_catalog_agg AS (
   SELECT
       global_entity_id,
       sku,
       product_name,
       CAST(supplier_id AS STRING) AS supplier_id,
       sup_id_parent,
       CASE WHEN sup_id_parent IS NULL THEN 'Division' ELSE NULL END AS division_type,
       CASE WHEN CAST(supplier_id AS STRING) = sup_id_parent
           THEN TRUE END AS is_sup_id_parent,
       all_descendant_supplier_ids_string,
       global_supplier_id,
       brand_name,
       brand_owner_name,
       level_zero,
       level_one,
       level_two,
       level_three,
       region_code,
       region_name
   FROM
       sku_sup_warehouse_qc_catalog
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
 SELECT
   ssw.*,
   s.country_code,
   qc.* EXCEPT (global_entity_id, sku, supplier_id)
 FROM sku_sup_warehouse AS ssw
 LEFT JOIN sku_sup_qc_catalog_agg AS qc
   ON ssw.global_entity_id = qc.global_entity_id
   AND ssw.sku_id = qc.sku
   AND ssw.supplier_id = qc.supplier_id
 LEFT JOIN sources AS s
   ON ssw.global_entity_id = s.global_entity_id




-------- CHECKS ---------


-- where ssw.sku_id = '500006'


-- select * from sku_sup_warehouse_purch_ord where sku_id = '500006'


-- SELECT * FROM `dh-central-salesforce-tech.salesforce_platform_analytics_staging.sps_product`
-- where sku_id = '906214'
-- and warehouse_id = '7ab4052a-33e4-4e52-9d47-844b61753c6c';




-- SELECT
--     global_entity_id,
--     sku_id,
--     warehouse_id,
--     -- Count the number of distinct suppliers for this grouping key
--     COUNT(DISTINCT supplier_id) AS distinct_supplier_count,
--     -- Flag if more than one supplier exists
--     CASE
--         WHEN COUNT(DISTINCT supplier_id) > 1 THEN TRUE
--         ELSE FALSE
--     END AS has_multiple_suppliers
-- FROM
--     `dh-central-salesforce-tech.salesforce_platform_analytics_staging.sps_product`
-- -- If you need to filter the base table first, add a WHERE clause here:
-- -- WHERE sku_id = '500006'
-- GROUP BY
--     global_entity_id,
--     sku_id,
--     warehouse_id
-- HAVING
--     -- Filter to show only the cases where there is more than one supplier
--     COUNT(DISTINCT supplier_id) > 1
-- ORDER BY
--     distinct_supplier_count DESC;


