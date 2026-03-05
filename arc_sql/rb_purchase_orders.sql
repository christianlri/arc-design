--This table aggregate all product-related information, including country, SKU, supplier, and warehouse-level data, essential for rebate calculations.

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.rb_purchase_orders`
AS 
WITH sources AS (
    SELECT s.timezone,
    s.country_code
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sources` AS s
    GROUP BY ALL
),
srm_excise_tax AS (
    SELECT 
    gset.country_code,
    gset.sku,
    gset.tax AS excise_tax,
    gset.start_date, 
    gset.end_date,
    gset.type,
    FROM `{{ params.project_id }}.{{ params.dataset.dl }}.gsheet_gsh_srm_excise_tax` AS gset
    GROUP BY ALL
)
SELECT
    po.country_code,
    po.po_order_uuid AS order_id,
    po.po_order_id AS po_id,
    po.parent_purchase_order_uuid AS parent_purchase_order_id,
    po.created_localtime_at AS created_date_local,
    po.created_localtime_at AS created_date,
    CASE 
        WHEN REGEXP_CONTAINS(po.country_code, {{ params.param_inbounded_country_code }}) THEN COALESCE(DATETIME(rec.inbound_received_at), DATETIME(po.fulfilled_localtime_at))
        ELSE DATETIME(po.fulfilled_localtime_at)
    END AS order_delivery_date,
    po.order_status AS purchase_order_status,
    po.warehouse_id,
    po.warehouse.warehouse_name AS warehouse_name,
    wv2.is_dmart,
    CASE
        WHEN wv2.is_dmart IS TRUE THEN 'DMART'
        ELSE 'DISTRIBUTION CENTER'
    END AS warehouse_type,
    po.supplier_id AS sup_id,
    pp.product_name,
    pp.sku_id AS sku,
    gset.excise_tax,
    CASE 
        WHEN gset.excise_tax IS NULL THEN pp.item_values_lc.unit_cost_after_discount_lc 
        WHEN gset.type = 'Absolute' THEN pp.item_values_lc.unit_cost_after_discount_lc - COALESCE(gset.excise_tax, 0)
        WHEN gset.type = 'Percentage' THEN pp.item_values_lc.unit_cost_after_discount_lc  * (1 - COALESCE(gset.excise_tax, 0))
        ELSE pp.item_values_lc.unit_cost_after_discount_lc
    END AS gross_cost_without_vat,
    CASE 
        WHEN gset.excise_tax IS NULL THEN  pp.item_values_lc.unit_cost_after_discount_with_vat_lc 

        ELSE
        (
            CASE 
            WHEN gset.type = 'Absolute' THEN  pp.item_values_lc.unit_cost_after_discount_lc  - COALESCE(gset.excise_tax, 0)
            WHEN gset.type = 'Percentage' THEN pp.item_values_lc.unit_cost_after_discount_lc * (1 - COALESCE(gset.excise_tax, 0))

            ELSE pp.item_values_lc.unit_cost_after_discount_lc
            END
        )
        * SAFE_DIVIDE(
            pp.item_values_lc.unit_cost_after_discount_with_vat_lc,
            pp.item_values_lc.unit_cost_after_discount_lc
            )
    END AS gross_cost,
    pp.item_values_eur.unit_cost_after_discount_with_vat_eur AS gross_cost_eur,
    pp.item_values_eur.unit_cost_after_discount_eur AS gross_cost_without_vat_euro,
    CASE 
        WHEN REGEXP_CONTAINS(po.country_code, {{ params.param_inbounded_country_code }}) THEN COALESCE(rec.inbound_quantity, pp.quantity_delivered)
        ELSE pp.quantity_delivered
    END AS delivered_quantity,
    pp.quantity_ordered AS ordered_quantity,
    pp.product.vat_rate,
    ret.return_status,
    ret.reason,
    NULL AS return_id,
    ret.quantity AS returned_quantity,
    DATETIME(TIMESTAMP(ret.completed_at), s.timezone) AS return_completed_at,
    DATETIME(TIMESTAMP(ret.completed_at), s.timezone) AS return_completed_at_local,
    NULL AS trade_cost,
    NULL AS fixed_discount,
    NULL AS extra_discount,
    NULL AS fixed_vat,
    NULL AS extra_vat,
    NULL AS internal_tax,
    DATE_TRUNC (
        MIN(po.created_localtime_at) OVER (
            PARTITION BY
                pp.sku_id,
                po.country_code
        ),
        month
    ) AS min_sku_created_month,
    /*For the condition term 'New Launch Discount', we need to calculate the minimum creation date, which will be considered as the launch date per SKU*/
    DATE_TRUNC (
        MIN(po.created_localtime_at) OVER (
            PARTITION BY
                po.supplier_id,
                po.country_code
        ),
        month
    ) AS min_supp_created_month,
    /*For the listing fee condition, it will assist in calculating the launch of new SKUs at the supplier level.*/
    DATE_TRUNC (
        MIN(po.created_localtime_at) OVER (
            PARTITION BY
                pp.sku_id,
                po.supplier_id,
                po.country_code
        ),
        month
    ) AS min_sku_supp_created_month,
    /*For the listing fee condition, it will assist in calculating the launch of new SKUs.*/
    COUNT(DISTINCT pp.sku_id) OVER (
        PARTITION BY
            po.supplier_id,
            po.country_code
    ) AS count_sku_supp_12_month,
    /*For the listing fee condition, it will assist in calculating the launch of new SKUs at the supplier level.*/
FROM
    `{{ params.project_id }}.{{ params.dataset.cl }}.purchase_orders` AS po
    LEFT JOIN UNNEST (po.products_purchased) AS pp
    LEFT JOIN UNNEST (pp.receiving) AS rec
    LEFT JOIN UNNEST (pp.returns) AS ret
    LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.warehouses_v2` wv2 ON po.warehouse_id = wv2.warehouse_id
    AND po.country_code = wv2.country_code
    LEFT JOIN sources AS s ON po.country_code = s.country_code
    LEFT JOIN srm_excise_tax AS gset ON pp.sku_id = gset.sku
        AND po.country_code = gset.country_code
        AND ( CASE
          WHEN REGEXP_CONTAINS (po.country_code, {{ params.param_inbounded_country_code }})THEN
              COALESCE(DATETIME (TIMESTAMP(rec.inbound_received_at), s.timezone), (DATETIME (TIMESTAMP(po.fulfilled_localtime_at), s.timezone)))
          ELSE DATETIME (TIMESTAMP(po.fulfilled_localtime_at), s.timezone)
          END ) BETWEEN DATE_TRUNC(gset.start_date, day) AND DATE_TRUNC(gset.end_date, day) 
WHERE TRUE 
    AND REGEXP_CONTAINS (po.country_code, {{ params.param_country_code }})
    AND NOT REGEXP_CONTAINS(po.warehouse_id, {{ params.exclude_service_warehouses_param }})
GROUP BY ALL
