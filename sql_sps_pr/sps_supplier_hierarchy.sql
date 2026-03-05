--- full refresh ---
-- This table extracts and maintains the Division-Principal mapping required for generating Supplier Scorecards.
-- SPS Execution: Position No. 1
CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_supplier_hierarchy`
CLUSTER BY
   global_entity_id,
   supplier_id,
   root_id
AS
WITH RECURSIVE supplier_hierarchy AS (
   -- Anchor Member: Start with all base entities (the children).
   SELECT
       a.id AS original_child_id,
       a.global_entity_id,
       a.srm_supplierportalid__c AS original_supplier_id,
       a.parentid AS current_parent_id,
       a.parentid AS immediate_parent_id_map,
       a.id AS root_id
   FROM
       `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS a
   UNION ALL
   -- Recursive Member: Traverse up one level, scoped by global_entity_id.
   SELECT
       sh.original_child_id,
       sh.global_entity_id,
       sh.original_supplier_id,
       p.parentid AS current_parent_id,
       sh.immediate_parent_id_map,
       p.id AS root_id
   FROM
       supplier_hierarchy AS sh
   INNER JOIN
       `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS p
       ON sh.current_parent_id = p.id
       AND sh.global_entity_id = p.global_entity_id
   WHERE
       sh.current_parent_id IS NOT NULL
),
ultimate_parent_mapping AS (
   -- Identify the row where the ultimate parent has been found (current_parent_id is NULL).
   SELECT
       sh.original_child_id,
       sh.global_entity_id,
       sh.original_supplier_id,
       sh.immediate_parent_id_map,
       sh.root_id
   FROM
       supplier_hierarchy AS sh
   WHERE
       sh.current_parent_id IS NULL
),
srm_suppliers_with_root AS (
   -- Combine base account data with parent/root IDs derived from the recursion.
   SELECT
       a.global_entity_id,
       a.country_code,
       a.srm_supplierportalid__c AS supplier_id,
       a.srm_gsid__c AS global_supplier_id,
       a.name AS supplier_name,
       upm.original_child_id AS original_account_id,
       immediate_parent.srm_supplierportalid__c AS immediate_sup_id_parent,
       root_parent.srm_supplierportalid__c AS ultimate_sup_id_parent,
       upm.immediate_parent_id_map AS immediate_root_id_parent,
       upm.root_id AS ultimate_root_id_parent
   FROM
       `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS a
   INNER JOIN
       ultimate_parent_mapping AS upm
       ON a.id = upm.original_child_id
       AND a.global_entity_id = upm.global_entity_id
   LEFT JOIN
       `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS immediate_parent
       ON upm.immediate_parent_id_map = immediate_parent.id
       AND upm.global_entity_id = immediate_parent.global_entity_id
   LEFT JOIN
       `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS root_parent
       ON upm.root_id = root_parent.id
       AND upm.global_entity_id = root_parent.global_entity_id
),
-- Deduplicate the descendant structs before aggregation
unique_descendants AS (
   SELECT DISTINCT
       s1.ultimate_root_id_parent,
       s1.global_entity_id,
       s1.supplier_id AS descendant_supplier_id,
       s1.original_account_id AS descendant_account_id
   FROM
       srm_suppliers_with_root AS s1
   WHERE
       s1.ultimate_root_id_parent IS NOT NULL
       AND s1.supplier_id IS NOT NULL
),
-- 3. AGGREGATION: Creates a unique array of paired Supplier ID and Account ID for all descendants.
final_root_descendants AS (
   SELECT
       ud.ultimate_root_id_parent,
       ud.global_entity_id,
       -- Aggregate the pre-deduplicated data (no DISTINCT needed)
       ARRAY_AGG(
           STRUCT(
               ud.descendant_supplier_id AS supplier_id,
               ud.descendant_account_id AS account_id
           ) IGNORE NULLS
       ) AS all_descendants_paired
   FROM
       unique_descendants AS ud
   GROUP BY 1, 2
)
-- FINAL SELECT: Orders columns precisely and joins the paired array
SELECT
   srm.global_entity_id,
   srm.country_code,
   -- 1. CURRENT SUPPLIER & ACCOUNT ID (Child)
   srm.supplier_id,
   srm.global_supplier_id,
   srm.supplier_name,
   srm.original_account_id AS root_id,
   -- 2. IMMEDIATE PARENT
   srm.immediate_sup_id_parent,
   srm.immediate_root_id_parent,
   -- 3. ULTIMATE PARENT (Root)
   srm.ultimate_sup_id_parent,
   srm.ultimate_root_id_parent,
   CASE WHEN srm.supplier_id = srm.ultimate_sup_id_parent
       THEN TRUE END AS is_ultimate_sup_id_parent,
   -- 4. DESCENDANTS (The Full Paired Array)
   frd.all_descendants_paired
FROM
   srm_suppliers_with_root AS srm
LEFT JOIN
   final_root_descendants AS frd
   ON srm.ultimate_root_id_parent = frd.ultimate_root_id_parent
   AND srm.global_entity_id = frd.global_entity_id
