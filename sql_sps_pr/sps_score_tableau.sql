-- This table extracts and maintains the efficiency metrics mapping required for generating Supplier Scorecards. 
-- SPS Execution: Final Position 
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_score_tableau

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_score_tableau`
CLUSTER BY
   global_entity_id
AS 
WITH
  all_keys AS (
SELECT DISTINCT * FROM (
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_price_index`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_days_payable`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_financial_metrics`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_line_rebate_metrics`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_efficiency`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_listed_sku`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_shrinkage`
  UNION ALL
  SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_delivery_costs`
))
SELECT o.*,
 p.median_price_index,
 dpo.payment_days,
 dpo.doh,
 dpo.dpo,
 sfm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup),
 slrm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup, net_purchase),
 se.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup),
 listed.listed_skus,
 shrink.spoilage_value,
 shrink.retail_revenue,
 shrink.spoilage_rate,
 deliv.delivery_cost_eur,
 deliv.delivery_cost_local
FROM all_keys AS o
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_price_index` AS p
  ON o.global_entity_id = p.global_entity_id AND o.time_period = p.time_period AND o.time_granularity = p.time_granularity AND o.division_type = p.division_type AND o.supplier_level = p.supplier_level AND o.entity_key = p.entity_key AND o.brand_sup = p.brand_sup
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_days_payable` AS dpo
  ON o.global_entity_id = dpo.global_entity_id AND o.time_period = dpo.time_period AND o.time_granularity = dpo.time_granularity AND o.division_type = dpo.division_type AND o.supplier_level = dpo.supplier_level AND o.entity_key = dpo.entity_key AND o.brand_sup = dpo.brand_sup
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_financial_metrics` AS sfm
  ON o.global_entity_id = sfm.global_entity_id AND o.time_period = sfm.time_period AND o.time_granularity = sfm.time_granularity AND o.division_type = sfm.division_type AND o.supplier_level = sfm.supplier_level AND o.entity_key = sfm.entity_key AND o.brand_sup = sfm.brand_sup
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_line_rebate_metrics` AS slrm
  ON o.global_entity_id = slrm.global_entity_id AND o.time_period = slrm.time_period AND o.time_granularity = slrm.time_granularity AND o.division_type = slrm.division_type AND o.supplier_level = slrm.supplier_level AND o.entity_key = slrm.entity_key AND o.brand_sup = slrm.brand_sup
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_efficiency` AS se
  ON o.global_entity_id = se.global_entity_id AND o.time_period = se.time_period AND o.time_granularity = se.time_granularity AND o.division_type = se.division_type AND o.supplier_level = se.supplier_level AND o.entity_key = se.entity_key AND o.brand_sup = se.brand_sup
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_listed_sku` AS listed
  ON o.global_entity_id = listed.global_entity_id AND o.time_period = listed.time_period AND o.time_granularity = listed.time_granularity AND o.division_type = listed.division_type AND o.supplier_level = listed.supplier_level AND o.entity_key = listed.entity_key AND o.brand_sup = listed.brand_sup
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_shrinkage` AS shrink
  ON o.global_entity_id = shrink.global_entity_id AND o.time_period = shrink.time_period AND o.time_granularity = shrink.time_granularity AND o.division_type = shrink.division_type AND o.supplier_level = shrink.supplier_level AND o.entity_key = shrink.entity_key AND o.brand_sup = shrink.brand_sup
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_delivery_costs` AS deliv
  ON o.global_entity_id = deliv.global_entity_id AND o.time_period = deliv.time_period AND o.time_granularity = deliv.time_granularity AND o.division_type = deliv.division_type AND o.supplier_level = deliv.supplier_level AND o.entity_key = deliv.entity_key AND o.brand_sup = deliv.brand_sup
