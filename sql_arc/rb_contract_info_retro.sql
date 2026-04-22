--This table Integrate contracts details, opportunity terms, and contracts terms with SKU-level data.
{%- if not params.backfill %}
  {% 
    set param_month = (
      data_interval_end.replace(day=1) - macros.dateutil.relativedelta.relativedelta(months=1)
    ).strftime('%Y-%m-%d') 
  %}
{%- elif params.is_backfill_chunks_enabled %}
  {% set param_month = params.backfill_start_date %}
{%- endif %}

WITH 
brand AS (
  SELECT b.id AS brand_id,
    b.pim_brand_id__c AS pim_brand_id
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.brand__c` AS b
  GROUP BY ALL
),

account AS (
  SELECT a.srm_gsid__c AS global_supplier_id,
    a.global_entity_id,
    a.country_code,
    a.srm_country__c AS country,
    a.name AS supplier_name,
    COALESCE (a.srm_financesystemid__c, a.parent_supplier_finance_id__c) AS finance_system_id,
    a.srm_supplierportalid__c AS sup_id,
    a.id AS account_id,
    a.ownerid AS user_id
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS a
  GROUP BY ALL
),

user AS (
  SELECT u.id AS user_id,
    u.username AS account_owner
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.user` AS u
  GROUP BY ALL
),

inactive_contracts AS (
  SELECT
    DATE_TRUNC(DATE(c.lastmodifieddate), MONTH) AS last_modified_month,
    DATE_TRUNC(c.startdate, MONTH) AS contract_start_date,
    DATE_TRUNC(c.enddate, MONTH) AS contract_end_date,
    DATE_TRUNC(DATE(c.activateddate), MONTH) AS inactive_activated_date,
    c.country_code,
    c.global_entity_id,
    c.gsid__c AS global_supplier_id,
    c.accountid As account_id, 
    c.id AS contract_id,
    ct.id AS term_id,
    ct.brand_name__c AS term_brand_name,
    ct.global_term_name__c AS trading_term_name,
    ct.srm_termapplicability__c AS term_applicability,
    ct.srm_valuetype__c AS term_value_type,
    ct.srm_reconciliationfrequency__c,
    ct.srm_value__c AS rebate_term_value,
    ct.brand__c AS brand_id,
    ct.srm_brandcategory__c AS brand_category_name,
    ct.pim_id__c AS brand_category_pim, 
    ct.currencyisocode AS term_currency
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.contract` AS c
  LEFT JOIN `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.srm_contracttradingterm__c` AS ct
    ON c.id = ct.srm_contract__c
  WHERE c.status = 'Inactive'
    AND DATE_TRUNC(DATE(c.lastmodifieddate), MONTH) = DATE_TRUNC(DATE('{{ param_month }}'), MONTH)
  QUALIFY ROW_NUMBER() OVER (
  PARTITION BY c.gsid__c, ct.global_term_name__c, COALESCE(ct.brand__c, 'N/A'), COALESCE(ct.srm_brandcategory__c, 'N/A'), COALESCE(ct.pim_id__c, 'N/A'), COALESCE(ct.srm_valuetype__c, 'N/A')
  ORDER BY c.enddate DESC
  ) = 1
),

active_contracts AS (
  SELECT
    DATE_TRUNC(DATE(c.startdate), MONTH) AS last_modified_month,
    DATE_TRUNC(c.enddate, MONTH) AS contract_end_date,
    DATE_TRUNC(c.startdate, MONTH) AS contract_start_date,
    DATE_TRUNC(DATE(c.activateddate), MONTH) AS active_activated_date,
    c.country_code,
    c.global_entity_id,
    c.gsid__c AS global_supplier_id,
    c.accountid As account_id, 
    c.id AS contract_id,
    ct.id AS term_id,
    ct.brand_name__c AS term_brand_name,
    ct.global_term_name__c AS trading_term_name,
    ct.srm_valuetype__c AS term_value_type,
    ct.srm_termapplicability__c AS term_applicability,
    ct.srm_reconciliationfrequency__c,
    ct.srm_value__c AS rebate_term_value,
    ct.brand__c AS brand_id,
    ct.srm_brandcategory__c AS brand_category_name,
    ct.pim_id__c AS brand_category_pim, 
    ct.currencyisocode AS term_currency
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.contract` AS c
  LEFT JOIN `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.srm_contracttradingterm__c` AS ct
    ON c.id = ct.srm_contract__c
  WHERE c.status = 'Active'
), 

contract_diffs AS (
  SELECT
    i.last_modified_month AS inactive_last_modified_month,
    i.contract_end_date AS inactive_contract_end_date,
    a.contract_start_date AS active_contract_start_date,
    i.inactive_activated_date,
    a.active_activated_date,
    COALESCE(a.country_code, i.country_code) AS country_code,
    COALESCE(a.global_entity_id, i.global_entity_id) AS global_entity_id,
    COALESCE(a.account_id, i.account_id) AS account_id,
    COALESCE(a.global_supplier_id, i.global_supplier_id) AS global_supplier_id,
    COALESCE(a.trading_term_name, i.trading_term_name) AS trading_term_name,
    COALESCE(a.term_value_type, i.term_value_type) AS term_value_type,
    COALESCE(a.brand_id, i.brand_id) AS brand_id,
    COALESCE(a.term_brand_name, i.term_brand_name) AS term_brand_name,
    COALESCE(a.brand_category_name, i.brand_category_name) AS brand_category_name,
    COALESCE(a.brand_category_pim, i.brand_category_pim) AS brand_category_pim,
    COALESCE(a.term_applicability, i.term_applicability) AS term_applicability,
    COALESCE(a.term_currency, i.term_currency) AS term_currency,
    a.contract_id AS new_contract_id,
    a.term_id AS new_term_id,
    ROUND(SAFE_SUBTRACT(a.rebate_term_value, i.rebate_term_value), 4) AS rebate_delta
  FROM active_contracts a
  FULL OUTER JOIN inactive_contracts i
    ON a.global_supplier_id = i.global_supplier_id
    AND a.trading_term_name = i.trading_term_name
    AND COALESCE(a.brand_id, 'N/A') = COALESCE(i.brand_id, 'N/A')
    AND COALESCE(a.brand_category_name, 'N/A') = COALESCE(i.brand_category_name, 'N/A')
    AND COALESCE(a.brand_category_pim, 'N/A') = COALESCE(i.brand_category_pim, 'N/A')
    AND COALESCE(a.term_value_type, 'N/A') = COALESCE(i.term_value_type, 'N/A')
  WHERE ROUND(SAFE_SUBTRACT(a.rebate_term_value, i.rebate_term_value), 4) IS NOT NULL
),

deep_dive_contracts AS (
  SELECT 
    d.*,
    a.country,
    a.supplier_name,
    a.finance_system_id,
    a.sup_id,
    'Retro_Overlap' AS contract_term_type,
    u.account_owner,
    b.pim_brand_id,
    month_series AS impact_month
  FROM contract_diffs as d
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(
    -- START: The first month the NEW contract was technically active
    DATE_TRUNC(d.active_contract_start_date, MONTH), 
    -- END: The month BEFORE the system actually recorded the change
    DATE_TRUNC(DATE_SUB(d.inactive_last_modified_month, INTERVAL 1 MONTH), MONTH), 
    INTERVAL 1 MONTH
  )) AS month_series
  LEFT JOIN account AS a ON d.account_id = a.account_id
  LEFT JOIN user AS u ON a.user_id = u.user_id
  LEFT JOIN brand AS b ON d.brand_id = b.brand_id
  WHERE d.rebate_delta > 0
    -- Ensure we only run this if the update happened LATER than the start date
  AND (
    CASE 
      WHEN d.inactive_activated_date != d.active_activated_date 
        THEN d.active_contract_start_date < d.active_activated_date
      ELSE d.active_contract_start_date < d.inactive_last_modified_month
    END
  )
)

/* FINAL SELECT: Grouping to consolidate multiple overlap months into one row */
SELECT
  new_contract_id AS contract_id,
  CAST(NULL AS STRING) AS opp_id,
  term_currency,
  global_supplier_id,
  global_entity_id,
  'Active' AS contract_status,
  MIN(DATE_TRUNC(impact_month, MONTH)) AS contract_startdate,
  MAX(LAST_DAY(impact_month, MONTH)) AS contract_enddate,
  MAX(LAST_DAY(impact_month, MONTH)) AS contract_effective_enddate,
  new_term_id AS contract_term_id,
  CAST(NULL AS STRING) AS opp_term_id,
  CAST(NULL AS STRING) AS  unit_of_measure,
  trading_term_name,
  'Retro' AS trading_term_type,
  term_value_type,
  rebate_delta AS rebate_term_value,
  term_applicability,
  'Monthly' AS term_frequency,
  NULL AS waive_condition__c,
  NULL AS waive_value__c,
  MIN(DATE_TRUNC(impact_month, MONTH)) AS term_start_date,
  MIN(DATE_TRUNC(impact_month, MONTH)) AS term_start_date_adjusted,
  TRUE AS valid_terms,
  MAX(LAST_DAY(impact_month, MONTH)) AS term_end_date,
  term_brand_name,
  CAST(NULL AS STRING) AS  term_comments,
  pim_brand_id AS term_pim_brand_id,
  brand_category_pim,
  brand_category_name AS term_brand_category_name,
  country_code,
  country,
  supplier_name,
  finance_system_id,
  sup_id,
  account_id,
  contract_term_type,
  account_owner,
  'standard_terms' AS trading_term_cluster,
  CAST( NULL AS STRING) AS tier_term_rebate_type,
  CAST(NULL AS FLOAT64) AS  tier_term_rebate,
  CAST(NULL AS FLOAT64) AS tier_term_threshold,
  CAST(NULL AS STRING) AS tier_thresholdtype,
  CAST(NULL AS FLOAT64) AS  tier_term_number,
  CAST(NULL AS STRING) AS calculated_against,
  DATE('{{ param_month }}') AS month
FROM deep_dive_contracts
GROUP BY ALL