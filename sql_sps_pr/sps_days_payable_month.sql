-- This table extracts and maintains the days payable metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Position No. 9.1
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_days_payable_month

WITH
date_in AS (
  SELECT
    {%- if not params.backfill %}
    DATE(DATE_TRUNC(DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY), MONTH)) AS date_in
    {%- elif params.is_backfill_chunks_enabled %}
    DATE(DATE_TRUNC(CAST('{{ params.backfill_start_date }}' AS DATE), MONTH)) AS date_in
    {%- endif %}
)
, date_fin AS (
  SELECT
    {%- if not params.backfill %}
    CAST('{{ next_ds }}' AS DATE) AS date_fin
    {%- elif params.is_backfill_chunks_enabled %}
    CAST('{{ params.backfill_end_date }}' AS DATE) AS date_fin
    {%- endif %}
)
, pim AS (
    SELECT product_id, brand_owner_name
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.pim_product`
    GROUP BY 1, 2
  ),
  qc AS (
    SELECT 
      global_entity_id, 
      country_code, 
      sku AS sku_id, 
      pim_product_id, 
      brand_name
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products`
    WHERE REGEXP_CONTAINS(global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY 1, 2, 3, 4, 5
  ),
  products AS (
    SELECT 
      qc.global_entity_id, 
      qc.sku_id, 
      COALESCE( NULLIF(LOWER(qc.brand_name), 'unbranded'), '_unknown_' ) AS brand_name,
      COALESCE( NULLIF(LOWER(pim.brand_owner_name), 'unbranded'), '_unknown_' ) AS brand_owner_name
    FROM qc
    LEFT JOIN pim ON qc.pim_product_id = pim.product_id
  ),
  supplier_mapping AS (
    SELECT
      sm.global_entity_id,
      CAST(sm.supplier_id AS STRING) AS supplier_id,
      ANY_VALUE(sm.sup_id_parent) AS principal_supplier_id,
      sm.division_type
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product`  AS sm
    WHERE REGEXP_CONTAINS(sm.global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY ALL
  ),
  account AS (
    SELECT
      a.global_entity_id,
      a.srm_supplierportalid__c AS supplier_id,
      CASE
          WHEN bank_payment_term__c = 'Pay immediately (Z000)' THEN 0
          WHEN bank_payment_term__c IN ('Pay immediately w/o deduction (0001)', '0001') THEN 1
          WHEN bank_payment_term__c = 'Vendor payment split 50-50 (DHK) (A020)' THEN 20
          WHEN bank_payment_term__c IN ('0002', '0018', '0019') THEN 7
          WHEN bank_payment_term__c IN ('0003', '0020', '0021', '0022') THEN 10
          WHEN bank_payment_term__c IN ('0004', 'Z114') THEN 14
          WHEN bank_payment_term__c IN ('0005', '0023', '0025', '0026', '0028', '0030', 'Z130') THEN 30
          WHEN bank_payment_term__c IN ('0006', '0033') THEN 45
          WHEN bank_payment_term__c IN ('0007', '0024', '0027', '0029', '0031', '0035') THEN 60
          WHEN bank_payment_term__c = '0008' THEN 90
          WHEN bank_payment_term__c = '0009' THEN 120
          WHEN bank_payment_term__c = '0010' THEN 150
          WHEN bank_payment_term__c = '0011' THEN 180
          WHEN bank_payment_term__c = '0012' THEN 210
          WHEN bank_payment_term__c = '0013' THEN 240
          WHEN bank_payment_term__c = '0014' THEN 270
          WHEN bank_payment_term__c = '0015' THEN 300
          WHEN bank_payment_term__c = '0016' THEN 330
          WHEN bank_payment_term__c = '0017' THEN 365
          WHEN bank_payment_term__c = '0032' THEN 21
          WHEN bank_payment_term__c = '0034' THEN 28
          WHEN bank_payment_term__c = 'Z115' THEN 15
          WHEN bank_payment_term__c LIKE '%days%' THEN CAST(REGEXP_EXTRACT(bank_payment_term__c, r'(\d+)') AS INT64)
          WHEN bank_payment_term__c LIKE '%End Of Month%' THEN CAST(REGEXP_EXTRACT(bank_payment_term__c, r'(\d+) Days') AS INT64)
          WHEN bank_payment_term__c LIKE '%from Invoice Date%' THEN CAST(REGEXP_EXTRACT(bank_payment_term__c, r'(\d+) Days') AS INT64)
          WHEN bank_payment_term__c LIKE 'Z0__' THEN CAST(REGEXP_EXTRACT(bank_payment_term__c, r'Z0(\d\d)') AS INT64)
          WHEN bank_payment_term__c LIKE 'Days_%_End_Of_Month' THEN CAST(REGEXP_EXTRACT(bank_payment_term__c, r'Days_(\d+)_End_Of_Month') AS INT64) 
          WHEN REGEXP_CONTAINS(bank_payment_term__c, r'^[A-Z]{1,2}\d+$') THEN CAST(REGEXP_EXTRACT(bank_payment_term__c, r'[A-Z]{1,2}(\d+)') AS INT64)
          ELSE NULL
      END AS payment_days,
    FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS a
    WHERE REGEXP_CONTAINS(a.country_code, {{ params.param_country_code }})
      AND a.bank_payment_term__c IS NOT NULL
    GROUP BY 1, 2, 3
  ),
  sku_stock_days_on_hand AS (
    SELECT
      sd.month,
      CAST(DATE_TRUNC(sd.month, MONTH) AS STRING) AS stock_days_month,
      CAST(CONCAT('Q', EXTRACT(QUARTER FROM sd.month), '-', EXTRACT(YEAR FROM sd.month)) AS STRING) AS stock_days_quarter,
      sd.global_entity_id,
      sd.sku AS sku_id,
      COALESCE(CAST(sd.supplier_id AS STRING), '_unknown_' ) AS supplier_id,
      COALESCE(sd.level_one_category, '_unknown_' ) AS l1_master_category,
      COALESCE(sd.level_two_category, '_unknown_' ) AS l2_master_category,
      COALESCE(sd.level_three_category, '_unknown_' ) AS l3_master_category,
      DATE_DIFF(DATE_ADD(DATE(month), INTERVAL 1 MONTH), DATE(month), DAY) AS days_in_month,
      DATE_DIFF(DATE_ADD(DATE_TRUNC(DATE(month), QUARTER), INTERVAL 1 QUARTER), DATE_TRUNC(DATE(month), QUARTER), DAY) AS days_in_quarter,
      SUM(sd.month_end_stock_value_eur) sku_month_end_stock_value_eur,
      SUM(sd.cogs_eur_monthy) AS sku_cogs_eur_monthy,
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.stock_days_on_hand` AS sd
    WHERE REGEXP_CONTAINS(sd.global_entity_id, {{ params.param_global_entity_id }})
      AND (sd.month BETWEEN (SELECT date_in FROM date_in).date_in AND (SELECT date_fin FROM date_fin).date_fin)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
  )
    SELECT
      sd.*,
      p.brand_owner_name,
      p.brand_name,
      sm.principal_supplier_id,
      sm.division_type,
      a.payment_days,
      CASE
        WHEN DATE_TRUNC(CAST(sd.month AS DATE), MONTH) = DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH)
        THEN CAST('{{ next_ds }}' AS DATE)
          ELSE LAST_DAY(CAST(sd.month AS DATE))
      END AS partition_month,
    FROM sku_stock_days_on_hand AS sd
    LEFT JOIN account AS a ON sd.global_entity_id = a.global_entity_id AND sd.supplier_id = a.supplier_id
    LEFT JOIN supplier_mapping AS sm ON sd.global_entity_id = sm.global_entity_id AND sd.supplier_id = sm.supplier_id
    LEFT JOIN products AS p ON sd.sku_id = p.sku_id AND sd.global_entity_id = p.global_entity_id
