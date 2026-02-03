{%- if not params.backfill %}
  {% 
    set param_month = (
      data_interval_end.replace(day=1) - macros.dateutil.relativedelta.relativedelta(months=1)
    ).strftime('%Y-%m-%d') 
  %}
{%- elif params.is_backfill_chunks_enabled %}
  {% set param_month = params.backfill_start_date %}
{%- endif %}

WITH orders_current_month AS (
  SELECT o.order_id,
    o.sup_id,
    o.country_code,
    o.parent_purchase_order_id,
    o.purchase_order_status,
    o.created_date,
    o.order_delivery_date,
    o.warehouse_type,
    o.warehouse_id,
    FROM`{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders` AS o
  WHERE TRUE
    AND DATE_TRUNC(CAST(DATETIME(o.order_delivery_date) AS DATE), month) = '{{ param_month }}'
    AND o.purchase_order_status IN ('done', 'confirmed')
    /*order status 3 = done; 1 = confirmed*/
    AND REGEXP_CONTAINS(o.country_code, {{ params.param_country_code }})
    GROUP BY ALL
),
        /*ALL TERMS: This applies only to the previous month. Reason: All orders that were considered children last month and have now become parents need to be subtracted.*/
orders_parent_prev_month AS (
  SELECT op.order_id,
    op.parent_purchase_order_id,
    op.sup_id,
    op.country_code,
    op.purchase_order_status,
    o.created_date,
    op.order_delivery_date,
    op.warehouse_type,
    op.warehouse_id,
    FROM orders_current_month AS o
    LEFT JOIN`{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders` AS op ON o.parent_purchase_order_id = op.order_id
    WHERE TRUE
    AND op.purchase_order_status IN ('done', 'confirmed')
    /*order status 3 = done; 1 = confirmed*/
    AND o.parent_purchase_order_id IS NOT NULL
    AND DATE_TRUNC(op.order_delivery_date, month) <= DATE_ADD(
      DATE_TRUNC('{{ param_month }}', month),
      INTERVAL - 1 MONTH
    )
    GROUP BY ALL
)
/*All orders that are not parents -> if they are not parents and were delivered -> order qualified; there is no limitation*/
SELECT 'regular' AS order_type,
  o.order_id,
  o.sup_id,
  o.country_code,
  o.purchase_order_status,
  o.order_delivery_date,
  o.warehouse_type,
  o.warehouse_id,
  DATE('{{ param_month }}') AS month
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders` AS o
WHERE TRUE
  AND o.purchase_order_status IN ('done')
  AND o.order_id NOT IN (
    SELECT DISTINCT o1.parent_purchase_order_id
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders` AS o1
    WHERE o1.parent_purchase_order_id IS NOT NULL
  )
  GROUP BY ALL
  /*no parrents assigned*/
UNION ALL
/*All orders that have parents (these are child orders) -> all orders that have parents and are delivered this month; example: price, quantity update; these updated orders are not included in the first category*/
SELECT 'last_updated_po' AS order_type,
  o.order_id,
  o.sup_id,
  o.country_code,
  o.purchase_order_status,
  o.order_delivery_date,
  o.warehouse_type,
  o.warehouse_id,
  DATE('{{ param_month }}') AS month
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders` AS o
WHERE TRUE
  AND o.purchase_order_status IN ('done', 'confirmed')
  AND o.parent_purchase_order_id IS NOT NULL
  AND DATE_TRUNC(o.order_delivery_date, month) = DATE_ADD(DATE_TRUNC('{{ param_month }}', month), INTERVAL 0 MONTH)
  GROUP BY ALL
UNION ALL
/*All orders that were children last month and became parents -> we have to subtract these orders*/
SELECT 'original_of_last_updated_po' AS order_type,
  op.order_id,
  op.sup_id,
  op.country_code,
  op.purchase_order_status,
  op.order_delivery_date,
  op.warehouse_type,
  op.warehouse_id,
  DATE('{{ param_month }}') AS month
  FROM orders_parent_prev_month AS op
  GROUP BY ALL
UNION ALL
SELECT 'updated_next_months' AS order_type,
  ocm.order_id,
  ocm.sup_id,
  ocm.country_code,
  ocm.purchase_order_status,
  ocm.order_delivery_date,
  ocm.warehouse_type,
  ocm.warehouse_id,
  DATE('{{ param_month }}') AS month
FROM orders_current_month AS ocm
WHERE TRUE 
/*this is for the rerun - orders which were child during the param month and became parents in the next months*/
AND ocm.order_id IN (
    SELECT 
      o.parent_purchase_order_id,
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders` AS o
    WHERE TRUE
      AND DATE_TRUNC(o.order_delivery_date, month) > DATE_ADD(DATE_TRUNC('{{ param_month }}', month), INTERVAL 0 MONTH)
      AND o.parent_purchase_order_id IS NOT NULL   
    GROUP BY ALL 
    )
