{%- if not params.backfill %}
  {% 
    set param_month = (
      data_interval_end.replace(day=1) - macros.dateutil.relativedelta.relativedelta(months=1)
    ).strftime('%Y-%m-%d') 
  %}
{%- elif params.is_backfill_chunks_enabled %}
  {% 
    set param_month = params.backfill_start_date
  %}
{%- endif %}

SELECT
    term_comments, -- Equivalent to Comments__c in SRM
    global_entity_id, -- Equivalent to Global_Entity_Id__c in SRM
    country_code, -- Equivalent to Country_Code__c in SRM
    country, -- Equivalent to Country__c in SRM
    global_supplier_id, -- Equivalent to Global_Supplier_Id__c in SRM
    supplier_name, -- Equivalent to Supplier_Name__c in SRM
    finance_system_id, -- Equivalent to Finance_System_Id__c in SRM
    sup_id, -- Equivalent to Sup_Id__c in SRM
    unit_of_measure, -- Equivalent to Unit_of_Measure__c in SRM
    trading_term_name, -- Equivalent to Trading_Term_Name__c in SRM
    trading_term_type, -- Equivalent to Trading_Term_Type__c in SRM
    rebate_term_value, -- Equivalent to Rebate_Term_Value__c in SRM
    term_start_date, -- Equivalent to Term_Start_Date__c in SRM
    term_end_date, -- Equivalent to Term_End_Date__c in SRM
    month, -- Equivalent to Month__c in SRM
    rebate_currency, -- Equivalent to Currency__c in SRM
    account_id, -- Equivalent to Account__c in SRM
    contract_id, -- Equivalent to Contract__c in SRM
    contract_term_id, -- Equivalent to Contract_Trading_Term__c in SRM
    opp_term_id, -- Equivalent to Opportunity_Trading_Term__c in SRM
    term_value_type, -- Equivalent to Term_Value_Type__c in SRM
    contract_status, -- Equivalent to Contract_Status__c in SRM
    term_applicability, -- Equivalent to Term_Applicability__c in SRM
    term_frequency, -- Equivalent to Reconciliation_Frequency__c in SRM
    contract_startdate, -- Equivalent to Contract_Start_Date__c in SRM
    contract_enddate, -- Equivalent to Contract_End_Date__c in SRM
    opp_id, -- Equivalent to Opportunity__c in SRM
    contract_term_type, -- Equivalent to Contract_Term_Type__c in SRM
    account_owner, -- Equivalent to Account_Owner__c in SRM
    SUM(COALESCE(ROUND(delivered_gross_amount, 3), 0)) AS term_gross_purchase, -- Equivalent to Term_Gross_Puchase__c in SRM
    SUM(COALESCE(ROUND(total_rebate, 3), 0)) AS rebate, -- Equivalent to rebate__c in SRM
    MAX(COALESCE(ROUND(sup_net_purchase, 3), 0)) AS net_purchase, -- Equivalent to Net_Purchase__c in SRM
    SUM(COALESCE(ROUND(delivered_net_amount, 3), 0)) AS term_net_purchase, -- Equivalent to Term_Net_Purchase__c in SRM
    MAX(COALESCE(ROUND(sup_gross_purchase, 3), 0)) AS gross_purchase, -- Equivalent to Gross_Purchase__c in SRM
    MAX(COALESCE(ROUND(sup_gross_return, 3), 0)) AS gross_return, -- Equivalent to Gross_Return__c in SRM
    SUM(COALESCE(ROUND(returned_gross_amount, 3), 0)) AS term_gross_return, -- Equivalent to Term_Gross_Return__c in SRM
    MAX(COALESCE(ROUND(sup_term_valid_gross_purchase, 3), 0)) AS term_valid_gross_purchase, -- Equivalent to Term_Valid_Gross_Purchase__c in SRM
    MAX(COALESCE(ROUND(sup_term_valid_gross_return, 3), 0)) AS term_valid_gross_return, -- Equivalent to Term_Valid_Gross_Return__c in SRM
    MAX(COALESCE(ROUND(sup_term_valid_net_purchase, 3), 0)) AS term_valid_net_purchase, -- Equivalent to Term_Valid_Net_Purchase__c in SRM
    MAX(COALESCE(ROUND(sup_term_valid_net_return, 3), 0)) AS term_valid_net_return, -- Equivalent to Term_Valid_Net_Return__c in SRM
    SUM(COALESCE(ROUND(returned_net_amount, 3), 0)) AS term_total_return, -- Equivalent to Term_Total_Return__c in SRM
    MAX(COALESCE(ROUND(sup_net_return, 3), 0)) AS total_return -- Equivalent to Total_Return__c in SRM
FROM 
    `{{ params.project_id }}.{{ params.dataset.cl }}.rb_sku_line_rebate`
WHERE     
    month = '{{ param_month }}'
    AND global_entity_id IN UNNEST({{ params.param_global_entity_id }})
GROUP BY 
    term_comments,
    global_entity_id,
    country_code,
    country,
    global_supplier_id,
    supplier_name,
    finance_system_id,
    sup_id,
    unit_of_measure,
    trading_term_name,
    trading_term_type,
    rebate_term_value,
    term_start_date,
    term_end_date,
    month,
    rebate_currency,
    account_id,
    contract_id,
    contract_term_id,
    opp_term_id,
    term_value_type,
    contract_status,
    term_applicability,
    term_frequency,
    contract_startdate,
    contract_enddate,
    opp_id,
    contract_term_type,
    account_owner
