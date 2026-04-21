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

WITH products AS (
  SELECT
    p.sku,
    p.country_code,
    p.brand_name,
    p.categ_level_one,
    p.categ_level_two,
    p.categ_level_three,
    p.categ_level_four,
    p.categ_level_five,
    p.categ_level_six,
    p.master_created_at,
    p.barcodes,
  FROM
    `{{ params.project_id }}.{{ params.dataset.cl }}.rb_products` AS p
  WHERE TRUE
  GROUP BY ALL
),
monthly_sku_total AS (
  SELECT
    oss.country_code,
    oss.sup_id,
    oss.sku,
    p.brand_name,
    p.categ_level_one,
    p.categ_level_two,
    p.categ_level_three,
    p.categ_level_four,
    p.barcodes,
    DATE_TRUNC(oss.received_local_time, month) AS received_local_month,
    SUM(
      CAST(oss.gross_cost_without_vat AS float64) * CAST(oss.delivered_quantity AS int64)
    ) AS delivered_net_amount,
    SUM(
      CAST(oss.gross_cost_without_vat AS float64) * CAST(oss.returned_quantity AS int64)
    ) AS returned_net_amount,
    SUM(
      CAST(oss.gross_cost AS float64) * CAST(oss.delivered_quantity AS int64)
    ) AS delivered_gross_amount,
    SUM(
      CAST(oss.gross_cost AS float64) * CAST(oss.returned_quantity AS int64)
    ) AS returned_gross_amount,
  FROM
    `{{ params.project_id }}.{{ params.dataset.cl }}.rb_orderline_sku` AS oss
  LEFT JOIN
    products AS p
    ON oss.sku = p.sku AND oss.country_code = p.country_code
  WHERE TRUE
    AND oss.month = '{{ param_month }}'
    AND DATE_TRUNC(oss.received_local_time, month) = '{{ param_month }}'
  GROUP BY ALL
),
-- New CTE to calculate net_amount and gross_amount once
MonthlySKUCalculations AS (
    SELECT
        mo.country_code,
        mo.sup_id,
        mo.sku,
        mo.brand_name,
        mo.categ_level_one,
        mo.categ_level_two,
        mo.categ_level_three,
        mo.categ_level_four,
        mo.received_local_month,
        mo.delivered_net_amount,
        mo.returned_net_amount,
        mo.delivered_gross_amount,
        mo.returned_gross_amount,
        -- Calculate net_amount for each row (SKU-level)
        IFNULL(mo.delivered_net_amount, 0) - IFNULL(mo.returned_net_amount, 0) AS net_amount,
        -- Calculate gross_amount for each row (SKU-level) - this is the expression we'll reuse
        IFNULL(mo.delivered_gross_amount, 0) - IFNULL(mo.returned_gross_amount, 0) AS gross_amount
    FROM
        monthly_sku_total AS mo
)
SELECT
    calc.country_code,
    calc.sup_id,
    calc.sku,
    calc.brand_name,
    calc.categ_level_one,
    calc.categ_level_two,
    calc.categ_level_three,
    calc.categ_level_four,
    calc.received_local_month,
    calc.delivered_net_amount,
    calc.returned_net_amount,
    calc.delivered_gross_amount,
    calc.returned_gross_amount,
    calc.net_amount,  -- Reusing the calculated net_amount from the CTE
    calc.gross_amount, -- Reusing the calculated gross_amount from the CTE
    -- Total gross_amount for the current SKU's Supplier
    SUM(calc.gross_amount) OVER w_supplier AS total_gross_amount_by_supplier,
    -- Total gross_amount for the current SKU's Brand
    SUM(calc.gross_amount) OVER w_brand AS total_gross_amount_by_brand,
    -- Total gross_amount for the current SKU's Categ Level One
    SUM(calc.gross_amount) OVER w_categ1 AS total_gross_amount_by_categ1,
    -- Total gross_amount for the current SKU's Categ Level Two
    SUM(calc.gross_amount) OVER w_categ2 AS total_gross_amount_by_categ2,
    -- Total gross_amount for the current SKU's Categ Level Three
    SUM(calc.gross_amount) OVER w_categ3 AS total_gross_amount_by_categ3,
    -- Total gross_amount for the current SKU's Categ Level Four
    SUM(calc.gross_amount) OVER w_categ4 AS total_gross_amount_by_categ4,
    ------------------------------------------------------------------------------------------------------
    -- Calculate Shares for each SKU against its respective totals, using gross_amount
    ------------------------------------------------------------------------------------------------------
    -- Share of SKU inside the Supplier
    SAFE_DIVIDE(calc.gross_amount, SUM(calc.gross_amount) OVER w_supplier) AS share_sku_in_supplier,
    -- Share of SKU inside the Brand
    SAFE_DIVIDE(calc.gross_amount, SUM(calc.gross_amount) OVER w_brand) AS share_sku_in_brand,
    -- Share of SKU inside Categ Level One
    SAFE_DIVIDE(calc.gross_amount, SUM(calc.gross_amount) OVER w_categ1) AS share_sku_in_categ1,
    -- Share of SKU inside Categ Level Two
    SAFE_DIVIDE(calc.gross_amount, SUM(calc.gross_amount) OVER w_categ2) AS share_sku_in_categ2,
    -- Share of SKU inside Categ Level Three
    SAFE_DIVIDE(calc.gross_amount, SUM(calc.gross_amount) OVER w_categ3) AS share_sku_in_categ3,
    -- Share of SKU inside Categ Level Four
    SAFE_DIVIDE(calc.gross_amount, SUM(calc.gross_amount) OVER w_categ4) AS share_sku_in_categ4
FROM
    MonthlySKUCalculations AS calc
WINDOW
    w_supplier AS (PARTITION BY country_code, sup_id, received_local_month),
    w_brand AS (PARTITION BY country_code, brand_name, received_local_month),
    w_categ1 AS (PARTITION BY country_code, categ_level_one, received_local_month),
    w_categ2 AS (PARTITION BY country_code, categ_level_one, categ_level_two, received_local_month),
    w_categ3 AS (PARTITION BY country_code, categ_level_one, categ_level_two, categ_level_three, received_local_month),
    w_categ4 AS (PARTITION BY country_code, categ_level_one, categ_level_two, categ_level_three, categ_level_four, received_local_month)
