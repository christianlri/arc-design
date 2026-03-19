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

SELECT *
FROM (
 WITH all_orderline AS (
   SELECT
     DATE_TRUNC(ol.order_delivery_date, MONTH) AS month,
     'orderline' AS source_table,
     ol.country_code,
     ol.order_id,
     ol.po_id,
     ol.created_date,
     ol.sku,
     ol.sup_id,
     ol.gross_cost_without_vat,
     ol.gross_cost,
     ol.gross_cost_without_vat_euro,
     ol.gross_cost_eur,
     ol.delivered_quantity,
     ol.ordered_quantity,
     NULL AS returned_quantity,
     ol.trade_cost,
     ol.fixed_discount,
     ol.extra_discount,
     ol.fixed_vat,
     ol.extra_vat,
     ol.vat_rate,
     ol.internal_tax,
     ol.warehouse_type,
     ol.warehouse_id,
     ol.warehouse_name,
     ol.order_delivery_date,
     CAST(NULL AS STRING) AS reason,
     ol.return_id,
     CAST(NULL AS DATETIME) AS return_completed_at,
     ol.min_sku_created_month,
     ol.min_supp_created_month,
     ol.min_sku_supp_created_month,
     ol.count_sku_supp_12_month,
     ol.excise_tax
   FROM 
   `{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders` AS ol
   WHERE 
   TRUE
     AND (
       ol.purchase_order_status IN ('done', 'confirmed')
       OR (
         ol.purchase_order_status = 'partially_inbounded'
         AND REGEXP_CONTAINS(ol.country_code, {{ params.param_inbounded_country_code }})
       )
     )
   GROUP BY ALL
 ),
 /*ALL TERMS: all returnlines completed this month and in status 'done'*/
 all_returnline AS (
   SELECT
     DATE_TRUNC(ol.return_completed_at, MONTH) AS month,
     'returnline' AS source_table,
     ol.country_code,
     ol.order_id,
     ol.po_id,
     ol.created_date,
     ol.sku,
     ol.sup_id,
     ol.gross_cost_without_vat,
     ol.gross_cost,
     ol.gross_cost_without_vat_euro,
     ol.gross_cost_eur,
     ol.delivered_quantity,
     ol.ordered_quantity,
     ol.returned_quantity,
     ol.trade_cost,
     ol.fixed_discount,
     ol.extra_discount,
     ol.fixed_vat,
     ol.extra_vat,
     ol.vat_rate,
     ol.internal_tax,
     ol.warehouse_type,
     ol.warehouse_id,
     ol.warehouse_name,
     ol.reason,
     ol.return_id,
     ol.return_completed_at,
     ol.min_sku_created_month,
     ol.min_supp_created_month,
     ol.min_sku_supp_created_month,
     ol.count_sku_supp_12_month,
     ol.excise_tax,
     MAX(ol.order_delivery_date) AS order_delivery_date,
   FROM 
    `{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders` AS ol
   WHERE 
      TRUE
     AND ol.return_status IN ('done')
   GROUP BY ALL
   HAVING REGEXP_CONTAINS(
     CONCAT(ol.country_code, '-', LOWER(ol.reason)),
     {{ params.po_return_param_country_reason }}
   )
 ),
 /*ALL TERMS: allowlist originals that need to be subtracted in param_month even if delivered earlier*/
 allowlist_originals AS (
   SELECT DISTINCT order_id
   FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_nonstandard_orders`
   WHERE TRUE
     AND month = DATE('{{ param_month }}')
     AND order_type = 'original_of_last_updated_po'
 )
 /*FINAL SELECT FOR orderline_sku*/
 SELECT *
 FROM (
   SELECT
     'orderline' AS source_table,
     o.order_type,
     ol.country_code,
     o.sup_id,
     ol.order_id,
     ol.po_id,
     ol.sku,
     ol.gross_cost_without_vat,
     ol.gross_cost,
     /*The delivered_quantity should be subtracted in the case of original_of_last_updated_po and assigned to returned_quantity.*/
    CASE WHEN o.order_type = 'original_of_last_updated_po' THEN 0 
      ELSE ol.delivered_quantity END AS delivered_quantity,
     ol.ordered_quantity,
     /*The delivered_quantity should be subtracted in the case of original_of_last_updated_po and assigned to returned_quantity.*/
    CASE WHEN o.order_type = 'original_of_last_updated_po' THEN ol.delivered_quantity 
      ELSE 0 END AS returned_quantity,
     ol.trade_cost,
     ol.fixed_discount,
     ol.extra_discount,
     ol.fixed_vat,
     ol.extra_vat,
     ol.vat_rate,
     ol.internal_tax,
     ol.created_date,
     /*The received_local_time should be changed to param_month so that original_of_last_updated_po can be considered for subtraction.*/
     CASE
       WHEN o.order_type = 'original_of_last_updated_po' THEN CAST(DATE('{{ param_month }}') AS DATETIME)
       ELSE ol.order_delivery_date
     END AS received_local_time,
     ol.min_sku_created_month,
     ol.min_supp_created_month,
     ol.min_sku_supp_created_month,
     ol.count_sku_supp_12_month,
     o.warehouse_type,
     o.warehouse_id,
     ol.warehouse_name,
     CAST(NULL AS STRING) AS fixing_condition,
     CAST(NULL AS STRING) AS reason,
     ol.excise_tax,
     DATE('{{ param_month }}') AS month,
   FROM all_orderline AS ol
   JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.rb_nonstandard_orders` AS o
     ON o.order_id = ol.order_id
    AND ol.order_delivery_date = o.order_delivery_date
    /*GATEKEEPER: keep only nonstandard rows for the current param_month*/
    AND o.month = DATE('{{ param_month }}')
    AND REGEXP_CONTAINS(o.country_code, {{ params.param_country_code }})
   WHERE TRUE
     /*Only bring the current month + allowlisted originals for subtraction*/
     AND (
       ol.month = DATE('{{ param_month }}')
       OR EXISTS (SELECT 1 FROM allowlist_originals ao WHERE ao.order_id = ol.order_id)
     )
 ) o2
 /*Only keep the rows that should be accounted in param_month*/
 WHERE DATE_TRUNC(DATE(o2.received_local_time), MONTH) = DATE('{{ param_month }}')

 UNION ALL

 SELECT
   'stock_movement' AS source_table,
   'stock_movement' AS order_type,
   sm.country_code,
   sm.sup_id,
   '' AS order_id,
   '' AS po_id,
   sm.sku,
   sm.gross_cost_without_vat,
   sm.gross_cost,
   0 AS delivered_quantity,
   0 AS ordered_quantity,
   sm.returned_quantity AS returned_quantity,
   0 AS trade_cost,
   0 AS fixed_discount,
   0 AS extra_discount,
   0 AS fixed_vat,
   0 AS extra_vat,
   sm.vat_rate,
   0 AS internal_tax,
   NULL AS created_date,
   sm.received_local_time,
   NULL AS min_sku_created_month,
   NULL AS min_supp_created_month,
   NULL AS min_sku_supp_created_month,
   NULL AS count_sku_supp_12_month,
   sm.warehouse_type,
   sm.warehouse_id,
   sm.warehouse_name,
   sm.fixing_condition,
   sm.reason,
   sm.excise_tax,
   DATE('{{ param_month }}') AS month
 FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_stock_movement` AS sm
 WHERE TRUE
   AND sm.month = DATE('{{ param_month }}')

 UNION ALL

 SELECT
   'returnline' AS source_table,
   'return' AS order_type,
   rl.country_code,
   rl.sup_id,
   rl.order_id,
   rl.po_id,
   rl.sku,
   rl.gross_cost_without_vat,
   rl.gross_cost,
   0 AS delivered_quantity,
   0 AS ordered_quantity,
   rl.returned_quantity,
   rl.trade_cost,
   rl.fixed_discount,
   rl.extra_discount,
   rl.fixed_vat,
   rl.extra_vat,
   rl.vat_rate,
   rl.internal_tax,
   rl.created_date,
   rl.return_completed_at AS received_local_time,
   NULL AS min_sku_created_month,
   NULL AS min_supp_created_month,
   NULL AS min_sku_supp_created_month,
   NULL AS count_sku_supp_12_month,
   rl.warehouse_type,
   rl.warehouse_id,
   rl.warehouse_name,
   NULL AS fixing_condition,
   rl.reason,
   rl.excise_tax,
   DATE('{{ param_month }}') AS month
 FROM all_returnline AS rl
 /*NOT REDUNDANT: filter month here to prevent snapshot replication*/
 WHERE rl.month = DATE('{{ param_month }}')
)
