--This consolidated table captures all essential product information—including country, SKU, supplier, PIM ID, and warehouse-level data

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.rb_products`
PARTITION BY DATE(master_created_at)
AS 
WITH
  --------------------------------------------------------------------------------
  -- CTE 1: Selects the preferred version of PIM products
  --------------------------------------------------------------------------------
  pim_products AS (
    SELECT
      region_code,
      product_id,
      brand_uuid,
      brands_name AS brand_name,
      bln.brand_name_local AS brand_name_local,
      category_uuid,
      product_updated_at AS pim_product_updated_at
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.pim_product`
    LEFT JOIN UNNEST (brand_local_names) AS bln
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER ( PARTITION BY region_code, product_id ORDER BY product_updated_at DESC, (brand_uuid IS NOT NULL) DESC, (category_uuid IS NOT NULL)) = 1
  ),
  --------------------------------------------------------------------------------
  -- CTE 2: Processes QC Catalog Products, unwrapping arrays and applying filters
  --------------------------------------------------------------------------------
  qc_products AS (
    SELECT
      qcp.sku,
      MAX(qcp.product_name) AS sku_name,
      qcp.pim_product_id,
      qcp.global_entity_id,
      qcp.country_code,
      qcp.brand_name,
      mc.master_category_names.level_one   AS categ_level_one,
      mc.master_category_names.level_two   AS categ_level_two,
      mc.master_category_names.level_three AS categ_level_three,
      mc.master_category_names.level_four  AS categ_level_four,
      mc.master_category_names.level_five  AS categ_level_five,
      mc.master_category_names.level_six   AS categ_level_six,
      mc.master_category_names.level_seven AS categ_level_seven,
      mc.master_category_names.level_eight AS categ_level_eight,
      mc.master_category_names.level_nine  AS categ_level_nine,
      mc.master_category_names.level_ten   AS categ_level_ten,
      mc.master_category_ids.level_one     AS categ_level_one_pim_id,
      mc.master_category_ids.level_two     AS categ_level_two_pim_id,
      mc.master_category_ids.level_three   AS categ_level_three_pim_id,
      mc.master_category_ids.level_four    AS categ_level_four_pim_id,
      mc.master_category_ids.level_five    AS categ_level_five_pim_id,
      mc.master_category_ids.level_six     AS categ_level_six_pim_id,
      mc.master_category_ids.level_seven   AS categ_level_seven_pim_id,
      mc.master_category_ids.level_eight   AS categ_level_eight_pim_id,
      mc.master_category_ids.level_nine    AS categ_level_nine_pim_id,
      mc.master_category_ids.level_ten     AS categ_level_ten_pim_id,
      STRING_AGG(DISTINCT bc.barcode ORDER BY bc.barcode) AS barcodes,
      chain_product_created_at_utc,
      qcp.catalog_master_product_id,
      qcp.product_name,
      qcp.product_name_local,
      qcp.product_image_url,
      qcp.is_bundle,
      qcp.catalog_chain_id,
      qcp.catalog_chain_name,
      vp.vat_rate,
      qcp.master_product_created_at_utc AS master_created_at,
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` AS qcp
      LEFT JOIN UNNEST(qcp.master_categories) AS mc
      LEFT JOIN UNNEST(qcp.barcodes) AS bc
      LEFT JOIN UNNEST(qcp.vendor_products) AS vp
    WHERE TRUE
      AND vp.is_dmart
      AND REGEXP_CONTAINS(qcp.country_code, {{ params.param_country_code }})
    GROUP BY ALL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY qcp.global_entity_id, qcp.sku ORDER BY qcp.chain_product_created_at_utc, CONCAT(qcp.master_product_created_at_utc, MAX(qcp.product_name)) DESC NULLS LAST) = 1
  ),
  --------------------------------------------------------------------------------
  -- CTE 3: Selects the preferred product-supplier-warehouse link
  --------------------------------------------------------------------------------
  products_suppliers AS (
    SELECT
      ps.country_code,
      ps.global_entity_id,
      ps.sku,
      CAST(s.supplier_id AS STRING) AS sup_id,
      s.supplier_name,
      w.warehouse_id,
      w.is_dmart,
      w.store_id,
      w.start_date,
      s.is_preferred
    FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared }}.products_suppliers` AS ps
      LEFT JOIN UNNEST(ps.suppliers) AS s
      LEFT JOIN UNNEST(s.warehouses) AS w
    WHERE TRUE
      AND s.is_preferred
      AND REGEXP_CONTAINS(ps.country_code, {{ params.param_country_code }})
    GROUP BY ALL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.global_entity_id, ps.sku, w.warehouse_id, w.store_id ORDER BY w.start_date DESC NULLS LAST) = 1
  ),
  --------------------------------------------------------------------------------
  -- CTE 4: Maps QC products to the most frequent region_code from PIM
  --------------------------------------------------------------------------------
  region_mapping AS (
    SELECT
      rm.pim_region_code AS region_code,
      rm.global_entity_id,
    FROM `dh-darkstores-live.cl_data_science_qcommerce.pim_region_entity_mapping` AS rm
    WHERE TRUE 
    GROUP BY ALL
  ),
  --------------------------------------------------------------------------------
  -- CTE 5: Adds the region_code from region_mapping to QC products
  --------------------------------------------------------------------------------
  qc_products_region AS (
    SELECT
      qcp.*,
      rm.region_code,
    FROM qc_products AS qcp
      LEFT JOIN region_mapping AS rm
        ON qcp.global_entity_id = rm.global_entity_id
  )
  --------------------------------------------------------------------------------
  -- CTE 6: Final join of QC products, supplier info, and PIM details
  --------------------------------------------------------------------------------
    SELECT
      qcp.*,
      ps.sup_id,
      ps.supplier_name,
      ps.warehouse_id,
      ps.store_id,
      ps.start_date,
      ps.is_dmart,
      pim.region_code AS pim_region_id,
      pim.brand_uuid AS pim_brand_id,
      pim.category_uuid AS pim_category_id,
      pim.brand_name_local,
      ARRAY(
        SELECT DISTINCT NORMALIZE_AND_CASEFOLD(TRIM(categ_pim))
        FROM UNNEST([
          CAST(categ_level_one_pim_id   AS STRING),
          CAST(categ_level_two_pim_id   AS STRING),
          CAST(categ_level_three_pim_id AS STRING),
          CAST(categ_level_four_pim_id  AS STRING),
          CAST(categ_level_five_pim_id  AS STRING),
          CAST(categ_level_six_pim_id   AS STRING),
          CAST(categ_level_seven_pim_id AS STRING),
          CAST(categ_level_eight_pim_id AS STRING),
          CAST(categ_level_nine_pim_id  AS STRING),
          CAST(categ_level_ten_pim_id   AS STRING),
          CAST(pim.category_uuid AS STRING) 
        ]) AS categ_pim
        WHERE categ_pim IS NOT NULL AND categ_pim <> ''
      ) AS categories_all_pim_ids_norm  
    FROM qc_products_region AS qcp
      LEFT JOIN products_suppliers AS ps
        ON ps.global_entity_id = qcp.global_entity_id
        AND ps.sku = qcp.sku
      LEFT JOIN pim_products AS pim
        ON qcp.pim_product_id = pim.product_id
        AND qcp.region_code = pim.region_code
