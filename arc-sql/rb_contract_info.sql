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

/*ALL TERMS: adding official contract information*/
WITH contract AS (
  SELECT *
  EXCEPT (rank_filter_contracts, contract_status),
    CASE
      WHEN contract_status = 'Inactive' THEN 'Active - Extended'
      ELSE contract_status
    END AS contract_status
  FROM (
      SELECT c.id AS contract_id,
        c.gsid__c AS global_supplier_id,
        c.global_entity_id,
        c.status AS contract_status,
        c.startdate AS contract_startdate,
        c.enddate AS contract_enddate,
        c.effective_end_date__c AS contract_effective_enddate,
        c.srm_termtype__c AS contract_term_type,
        c.accountid AS account_id,
        /*filter to exclude unnecessary contracts: supplier with active and inactive -> exclude inactive; supplier with only inactive -> valid*/
        RANK() OVER (
          PARTITION BY c.global_entity_id,
          c.gsid__c
          ORDER BY (
              CASE
                WHEN c.status = 'Inactive' THEN 'Inactive'
                ELSE 'Active'
              END
            )
        ) AS rank_filter_contracts
      FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.contract` AS c
      WHERE TRUE
        AND REGEXP_CONTAINS(c.global_entity_id, {{ params.param_global_entity_id }})
        /* filter: status = 'active' or (status = 'active extended' and current month between terms start and end date')*/
        AND (
          c.status = 'Active - Extended'
          OR (
            c.status = 'Active'
            AND DATE_TRUNC('{{ param_month }}', month) BETWEEN DATE_TRUNC(c.startdate, month)
            AND DATE_TRUNC(c.enddate, month)
          )
          OR (c.status = 'Inactive'
          AND DATE_TRUNC('{{ param_month }}', month) <= DATE_TRUNC(c.effective_end_date__c, month))
        )
      ) AS temp
  WHERE rank_filter_contracts = 1
),
/*ALL TERMS: adding official contract terms infromation*/
contractterm AS (
  SELECT ct.gsid__c AS global_supplier_id,
    ct.global_entity_id,
    ct.srm_contract__c AS contract_id,
    ct.id AS term_id,
    ct.srm_unitofmeasure__c AS unit_of_measure,
    ct.global_term_name__c AS trading_term_name,
    ct.srm_termtype__c AS trading_term_type,
    ct.srm_comments__c,
    ct.srm_value__c AS rebate_term_value,
    ct.srm_valuetype__c AS term_value_type,
    ct.srm_termapplicability__c AS term_applicability,
    ct.srm_reconciliationfrequency__c AS term_frequency,
    ct.srm_startdate__c AS term_start_date,
    ct.srm_enddate__c AS term_end_date,
    ct.brand_name__c AS brand_name,
    ct.brand__c AS brand_id,
    ct.pim_id__c AS brand_category_pim,
    ct.srm_brandcategory__c AS brand_category_name,
    ct.currencyisocode AS term_currency,
    ct.waive_condition__c,
    ct.waive_value__c,
    CASE
      WHEN (
        REGEXP_CONTAINS(LOWER(ct.global_term_name__c), r'progressive')
      ) THEN 'progressive_terms'
      WHEN (
        REGEXP_CONTAINS(
          LOWER(ct.global_term_name__c),
          r'minimum service level rebate'
        )
      ) THEN 'min_serv_lvl_terms'
      WHEN (
        REGEXP_CONTAINS(
          LOWER(ct.global_term_name__c),
          r'new launch discount'
        )
      ) THEN 'new_launch_terms'
      WHEN (
        REGEXP_CONTAINS(
          LOWER(ct.global_term_name__c),
          r'new dmart location opening fee|supplier account opening fee|supplier new store opening promotional support'
        )
      ) THEN 'blocker_terms'
      ELSE 'standard_terms'
    END AS trading_term_cluster,
    /*max_term_start_date - based on the param month; this calculation needed for the case when we have many active-extanted terms, with different startdate -> this is how we decide what is the valid term which should be considered based on the valid startdate*/
    MAX(
      CASE
        WHEN ct.srm_startdate__c <= DATE('{{ param_month }}') THEN ct.srm_startdate__c
        ELSE NULL
      END
    ) OVER (
      PARTITION BY ct.global_entity_id,
      ct.gsid__c,
      ct.global_term_name__c,
      ct.srm_contract__c,
      ct.srm_reconciliationfrequency__c,
      ct.srm_brandcategory__c
    ) AS max_term_start_date
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.srm_contracttradingterm__c` AS ct
  WHERE TRUE
    AND REGEXP_CONTAINS(ct.global_entity_id, {{ params.param_global_entity_id }})
),
/*ALL TERMS: adding unofficial contract information - verbal agreements - opp tables*/
opp AS (
  SELECT o.id AS opp_id,
    o.accountid AS account_id,
    o.gsid__c,
    o.stagename,
    o.srm_startdate__c,
    o.srm_enddate__c
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.opportunity` AS o
  WHERE TRUE
    AND o.agreement_type__c = 'Informal (No Contract)'
    AND o.stagename = 'Accepted'
    AND REGEXP_CONTAINS(o.global_entity_id, {{ params.param_global_entity_id }})
  GROUP BY ALL
),
/*ALL TERMS: adding unofficial contract terms infromation - verbal agreements - opp table*/
opp_term AS(
  SELECT ot.gsid__c AS global_supplier_id,
    ot.global_entity_id,
    NULL AS contract_id,
    ot.id AS term_id,
    ot.srm_unitofmeasure__c AS unit_of_measure,
    ot.global_term_name__c AS trading_term_name,
    ot.srm_termtype__c AS trading_term_type,
    ot.srm_value__c AS rebate_term_value,
    ot.srm_valuetype__c AS term_value_type,
    ot.srm_comments__c,
    ot.srm_termapplicability__c AS term_applicability,
    ot.srm_reconciliationfrequency__c AS term_frequency,
    ot.srm_startdate__c AS term_start_date,
    ot.srm_enddate__c AS term_end_date,
    ot.brand_name__c AS brand_name,
    ot.brand__c AS brand_id,
    ot.pim_id__c AS brand_category_pim,
    ot.srm_brandcategory__c AS brand_category_name,
    ot.currencyisocode AS term_currency,
    ot.srm_opportunity__c AS opp_id,
    ot.waive_condition__c,
    ot.waive_value__c,
    CASE
      WHEN (
        REGEXP_CONTAINS(LOWER(ot.global_term_name__c), r'progressive')
      ) THEN 'progressive_terms'
      WHEN (
        REGEXP_CONTAINS(
          LOWER(ot.global_term_name__c),
          r'minimum service level rebate'
        )
      ) THEN 'min_serv_lvl_terms'
      WHEN (
        REGEXP_CONTAINS(
          LOWER(ot.global_term_name__c),
          r'new launch discount'
        )
      ) THEN 'new_launch_terms'
      WHEN (
        REGEXP_CONTAINS(
          LOWER(ot.global_term_name__c),
          r'new dmart location opening fee|supplier account opening fee|supplier new store opening promotional support'
        )
      ) THEN 'blocker_terms'
      ELSE 'standard_terms'
    END AS trading_term_cluster,
    /*max_term_start_date - based on the param month; this calculation needed for the case when we have many active-extanted terms, with different startdate -> this is how we decide what is the valid term which should be considered based on the valid startdate*/
    MAX(
      CASE
        WHEN ot.srm_startdate__c <= DATE('{{ param_month }}') THEN ot.srm_startdate__c
        ELSE NULL
      END
    ) OVER (
      PARTITION BY ot.global_entity_id,
      ot.gsid__c,
      ot.global_term_name__c,
      ot.srm_reconciliationfrequency__c, 
      ot.srm_brandcategory__c
    ) AS max_term_start_date
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.srm_opportunitytradingterm__c` AS ot
  WHERE TRUE
    AND REGEXP_CONTAINS(ot.global_entity_id, {{ params.param_global_entity_id }})
),
/*ALL TERMS: adding brand id info - for term_applicability*/
brand AS (
  SELECT b.id AS brand_id,
    b.pim_brand_id__c AS pim_brand_id,
    /*b.brand_name__c,*/
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.brand__c` AS b
  GROUP BY ALL
),
/*ALL TERMS: adding account information*/
account AS (
  SELECT a.srm_gsid__c AS global_supplier_id,
    a.global_entity_id,
    a.country_code,
    a.srm_country__c AS country,
    a.name AS supplier_name,
    a.srm_financesystemid__c,
    a.parent_supplier_finance_id__c,
    COALESCE (a.srm_financesystemid__c, a.parent_supplier_finance_id__c) AS finance_system_id,
    a.srm_supplierportalid__c AS sup_id,
    a.id AS account_id,
    a.ownerid AS user_id,
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS a
  WHERE TRUE
    AND REGEXP_CONTAINS(a.global_entity_id, {{ params.param_global_entity_id }})
  GROUP BY ALL
),
/*ALL TERMS: table needed for the accound owner*/
user AS (
  SELECT u.id AS user_id,
    u.username AS account_owner
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.user` AS u
  GROUP BY ALL
),
/*PROGRESSIVE AND SPECIAL TERMS: this is for the calculation of tresholds and tiers*/
contracttradingtermtier AS (
  SELECT ctt.global_entity_id,
    --ctt.id AS tier_term_id,
    ctt.srm_contracttradingterm__c AS term_id,
    ctt.srm_rebatetype__c AS tier_term_rebate_type,
    ctt.srm_rebate__c AS tier_term_rebate,
    ctt.srm_threshold__c AS tier_term_threshold,
    ctt.srm_thresholdtype__c AS tier_thresholdtype,
    ctt.srm_tiernumber__c AS tier_term_number,
    ctt.gsid__c AS global_supplier_id,
    ctt.calculation_against__c AS calculated_against,
  FROM `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.srm_contracttradingtermtier__c` AS ctt
  GROUP BY ALL
)
/*ALL TERMS: general contract information*/
SELECT *
FROM (
    SELECT c.contract_id,
      NULL AS opp_id,
      ct.term_currency,
      c.global_supplier_id,
      c.global_entity_id,
      c.contract_status,
      c.contract_startdate,
      c.contract_enddate,
      c.contract_effective_enddate,
      ct.term_id AS contract_term_id,
      NULL AS opp_term_id,
      ct.unit_of_measure,
      ct.trading_term_name,
      ct.trading_term_type,
      ct.term_value_type,
      ct.rebate_term_value,
      ct.term_applicability,
      ct.term_frequency,
      ct.waive_condition__c,
      ct.waive_value__c,
      ct.term_start_date,
      CASE
        WHEN (
          c.contract_status = 'Active'
          AND ct.waive_condition__c = 'First Number of Months'
        ) THEN DATE_ADD(
          DATE_TRUNC(ct.term_start_date, month),
          INTERVAL CAST(ct.waive_value__c AS INT64) MONTH
        )
        ELSE ct.term_start_date
      END AS term_start_date_adjusted,
      /*ct.max_term_start_date,  -- for deep dive, juts in case*/
      CASE
        WHEN c.contract_status = 'Active' THEN TRUE
        WHEN c.contract_status = 'Active - Extended'
        AND ct.max_term_start_date = ct.term_start_date THEN TRUE
        ELSE FALSE
      END AS valid_terms,
      /*this calculation needed for the case when we have many active-extanted terms, with different startdate -> this is how we decide what is the valid term which should be considered based on the valid startdate*/
      ct.term_end_date,
      ct.brand_name AS term_brand_name,
      ct.srm_comments__c as term_comments,
      /*ct.brand_id,*/
      b.pim_brand_id AS term_pim_brand_id,
      ct.brand_category_pim,
      ct.brand_category_name AS term_brand_category_name,
      /*pim_category_id to be found and added to the terms table*/
      a.country_code,
      a.country,
      a.supplier_name,
      a.finance_system_id,
      a.sup_id,
      a.account_id,
      c.contract_term_type,
      u.account_owner,
      ct.trading_term_cluster,
      ctt.tier_term_rebate_type,
      ctt.tier_term_rebate,
      ctt.tier_term_threshold,
      ctt.tier_thresholdtype,
      ctt.tier_term_number,
      ctt.calculated_against,
      DATE('{{ param_month }}') as month
      FROM contract AS c
      JOIN contractterm AS ct ON c.global_supplier_id = ct.global_supplier_id
      AND c.global_entity_id = ct.global_entity_id
      AND c.contract_id = ct.contract_id
      LEFT JOIN contracttradingtermtier AS ctt ON ct.term_id = ctt.term_id
      AND ct.global_supplier_id = ctt.global_supplier_id
      AND ct.global_entity_id = ctt.global_entity_id
      LEFT JOIN account AS a ON c.account_id = a.account_id
      LEFT JOIN user AS u ON a.user_id = u.user_id
      LEFT JOIN brand AS b ON ct.brand_id = b.brand_id
    WHERE TRUE
    UNION ALL
    /*this is the union of offical contracts (contract_id not null) with the verbal agreements (op_id not null)*/
    SELECT NULL AS contract_id,
      o.opp_id,
      ot.term_currency,
      ot.global_supplier_id,
      ot.global_entity_id,
      o.stagename AS contract_status,
      o.srm_startdate__c AS contract_startdate,
      o.srm_enddate__c AS contract_enddate,
      NULL AS contract_effective_enddate,
      NULL AS contract_term_id,
      ot.term_id AS opp_term_id,
      ot.unit_of_measure,
      ot.trading_term_name,
      ot.trading_term_type,
      ot.term_value_type,
      ot.rebate_term_value,
      ot.term_applicability,
      ot.term_frequency,
      ot.waive_condition__c,
      ot.waive_value__c,
      ot.term_start_date,
      CASE
        WHEN (
          o.stagename = 'Active'
          AND ot.waive_condition__c = 'First Number of Months'
        ) THEN DATE_ADD(
          DATE_TRUNC(ot.term_start_date, month),
          INTERVAL CAST(ot.waive_value__c AS INT64) MONTH
        )
        ELSE ot.term_start_date
      END AS term_start_date_adjusted,
      /*ot.max_term_start_date, -- for deep dive, juts in case*/
      CASE
        WHEN o.stagename = 'Active' THEN TRUE
        WHEN o.stagename = 'Active - Extended'
        AND ot.max_term_start_date = ot.term_start_date THEN TRUE
        ELSE FALSE
      END AS valid_terms,
      /*this calculation needed for the case when we have many active-extanted terms, with different startdate -> this is how we decide what is the valid term which should be considered based on the valid startdate*/
      ot.term_end_date,
      ot.brand_name AS term_brand_name,
      ot.srm_comments__c as term_comments,
      /*ot.brand_id,*/
      b.pim_brand_id AS term_pim_brand_id,
      ot.brand_category_pim,
      ot.brand_category_name AS term_brand_category_name,
      /*pim_category_id to be found and added to the terms table*/
      a.country_code,
      a.country,
      a.supplier_name,
      a.finance_system_id,
      a.sup_id,
      a.account_id,
      'Informal' AS contract_term_type,
      u.account_owner,
      ot.trading_term_cluster,
      ctt.tier_term_rebate_type,
      ctt.tier_term_rebate,
      ctt.tier_term_threshold,
      ctt.tier_thresholdtype,
      ctt.tier_term_number,
      ctt.calculated_against,
      DATE('{{ param_month }}') as month
      FROM opp AS o
      JOIN opp_term AS ot ON o.opp_id = ot.opp_id
      JOIN contracttradingtermtier AS ctt ON ot.term_id = ctt.term_id
      AND ot.global_supplier_id = ctt.global_supplier_id
      AND ot.global_entity_id = ctt.global_entity_id
      LEFT JOIN account AS a ON o.account_id = a.account_id
      LEFT JOIN user AS u ON a.user_id = u.user_id
      LEFT JOIN brand AS b ON ot.brand_id = b.brand_id
    WHERE TRUE
  ) AS temp
WHERE valid_terms IS TRUE
