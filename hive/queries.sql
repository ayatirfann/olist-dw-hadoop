-- =====================================================================
-- Step 6: validation and business queries on the Olist star schema.
--
-- Part A checks that the star is sound (every fact key finds its
-- dimension row). Parts B-E ask business questions of each business
-- process, the way Ch. 3 of the Data Warehouse Toolkit tests the retail
-- star once it is built. Part F drills across two processes.
-- No totals, years or counts are hard-coded: every figure is computed
-- from the tables at run time.
--
-- Run:  ./run_hive.sh hive/queries.sql   (paths and database come from conf/olist.conf)
-- Note: in beeline a statement only runs when its line ENDS with ";"
--       (no comment after the semicolon).
-- =====================================================================
USE ${hivevar:DB};

-- ---------------------------------------------------------------------
-- A1. Referential integrity: fact keys with no dimension row.
--     Every count must be 0 (missing references point to the -1 row).
-- ---------------------------------------------------------------------
SELECT 'fact_order_item.product_key' AS fk, COUNT(*) AS orphans
FROM fact_order_item f LEFT JOIN dim_product d ON f.product_key = d.product_key
WHERE d.product_key IS NULL
UNION ALL
SELECT 'fact_order_item.seller_key', COUNT(*)
FROM fact_order_item f LEFT JOIN dim_seller d ON f.seller_key = d.seller_key
WHERE d.seller_key IS NULL
UNION ALL
SELECT 'fact_order_item.customer_key', COUNT(*)
FROM fact_order_item f LEFT JOIN dim_customer d ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL
UNION ALL
SELECT 'fact_order_item.purchase_date_key', COUNT(*)
FROM fact_order_item f LEFT JOIN dim_date d ON f.purchase_date_key = d.date_key
WHERE d.date_key IS NULL
UNION ALL
SELECT 'fact_order_fulfillment.delivered_date_key', COUNT(*)
FROM fact_order_fulfillment f LEFT JOIN dim_date d ON f.delivered_date_key = d.date_key
WHERE d.date_key IS NULL
UNION ALL
SELECT 'fact_order_fulfillment.customer_geo_key', COUNT(*)
FROM fact_order_fulfillment f LEFT JOIN dim_geography d ON f.customer_geo_key = d.geo_key
WHERE d.geo_key IS NULL
UNION ALL
SELECT 'fact_payment.payment_type_key', COUNT(*)
FROM fact_payment f LEFT JOIN dim_payment_type d ON f.payment_type_key = d.payment_type_key
WHERE d.payment_type_key IS NULL
UNION ALL
SELECT 'fact_review.review_created_date_key', COUNT(*)
FROM fact_review f LEFT JOIN dim_date d ON f.review_created_date_key = d.date_key
WHERE d.date_key IS NULL;

-- =====================================================================
-- B. Process: SELLING ITEMS  (fact_order_item, transaction fact)
-- =====================================================================

-- B1. Monthly sales trend (date dimension in its "purchase date" role).
SELECT d.year, d.month, d.month_name,
       COUNT(DISTINCT f.order_id)       AS orders,
       SUM(f.item_qty)                  AS items,
       SUM(f.price)                     AS revenue,
       SUM(f.freight_value)             AS freight
FROM fact_order_item f
JOIN dim_date d ON f.purchase_date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- B2. Top 10 product categories by revenue (English names from dim_product).
SELECT p.category_en,
       SUM(f.item_qty)                          AS items,
       SUM(f.price)                             AS revenue,
       ROUND(AVG(f.price), 2)                   AS avg_price,
       ROUND(100 * SUM(f.freight_value) / SUM(f.price), 1) AS freight_pct_of_price
FROM fact_order_item f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category_en
ORDER BY revenue DESC
LIMIT 10;

-- B3. Does freight cost depend on product weight? (dim_product.weight_band)
SELECT p.weight_band,
       SUM(f.item_qty)                          AS items,
       ROUND(AVG(f.price), 2)                   AS avg_price,
       ROUND(AVG(f.freight_value), 2)           AS avg_freight,
       ROUND(100 * SUM(f.freight_value) / SUM(f.price), 1) AS freight_pct_of_price
FROM fact_order_item f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.weight_band
ORDER BY avg_freight;

-- B4. Weekday vs weekend vs holiday: average revenue per calendar day.
--     Attributes like is_weekend / is_holiday exist only in dim_date,
--     not in the source; this is why the book insists on a date dimension.
SELECT day_type,
       COUNT(*)                     AS days,
       ROUND(AVG(revenue), 2)       AS avg_revenue_per_day,
       ROUND(AVG(orders), 1)        AS avg_orders_per_day
FROM (
  SELECT d.date_key,
         CASE WHEN d.is_holiday THEN 'holiday'
              WHEN d.is_weekend THEN 'weekend'
              ELSE 'weekday' END        AS day_type,
         SUM(f.price)                   AS revenue,
         COUNT(DISTINCT f.order_id)     AS orders
  FROM fact_order_item f
  JOIN dim_date d ON f.purchase_date_key = d.date_key
  GROUP BY d.date_key,
           CASE WHEN d.is_holiday THEN 'holiday'
                WHEN d.is_weekend THEN 'weekend'
                ELSE 'weekday' END
) daily
GROUP BY day_type
ORDER BY avg_revenue_per_day DESC;

-- B5. Seller region -> customer region (geography dimension in two roles).
SELECT sg.region AS seller_region, cg.region AS customer_region,
       SUM(f.item_qty) AS items,
       SUM(f.price)    AS revenue
FROM fact_order_item f
JOIN dim_geography sg ON f.seller_geo_key   = sg.geo_key
JOIN dim_geography cg ON f.customer_geo_key = cg.geo_key
GROUP BY sg.region, cg.region
ORDER BY revenue DESC
LIMIT 10;

-- B6. SCD Type 2 in action: revenue by the customer's state AT PURCHASE TIME
--     vs. the state they live in NOW. Rows differ only for customers who moved.
SELECT hist.state                                   AS state_at_purchase,
       cur.state                                    AS state_now,
       COUNT(DISTINCT hist.customer_unique_id)      AS customers,
       SUM(f.price)                                 AS revenue
FROM fact_order_item f
JOIN dim_customer hist ON f.customer_key = hist.customer_key
JOIN dim_customer cur  ON hist.customer_unique_id = cur.customer_unique_id
                      AND cur.is_current = true
WHERE hist.state <> cur.state
GROUP BY hist.state, cur.state
ORDER BY revenue DESC
LIMIT 10;

-- =====================================================================
-- C. Process: FULFILLING ORDERS  (fact_order_fulfillment, accumulating snapshot)
-- =====================================================================

-- C1. Where are orders in the pipeline? (dim_order_status)
SELECT s.order_status, s.is_terminal, s.is_successful,
       COUNT(*) AS orders,
       ROUND(100 * COUNT(*) / tot.n, 2) AS pct_of_orders
FROM fact_order_fulfillment f
JOIN dim_order_status s ON f.order_status_key = s.order_status_key
CROSS JOIN (SELECT COUNT(*) AS n FROM fact_order_fulfillment) tot
GROUP BY s.order_status, s.is_terminal, s.is_successful, tot.n
ORDER BY orders DESC;

-- C2. Average time in each pipeline stage, by purchase year.
--     Lag facts between milestones are the classic accumulating-snapshot
--     measures. Orders with impossible date sequences are excluded.
SELECT d.year,
       COUNT(*)                            AS delivered_orders,
       ROUND(AVG(f.approval_lag_days), 2)  AS avg_approval_days,
       ROUND(AVG(f.carrier_lag_days), 2)   AS avg_to_carrier_days,
       ROUND(AVG(f.delivery_lag_days), 2)  AS avg_carrier_to_customer_days,
       ROUND(AVG(f.total_lead_days), 2)    AS avg_total_days
FROM fact_order_fulfillment f
JOIN dim_date d ON f.purchase_date_key = d.date_key
WHERE f.is_delivered = 1 AND f.date_anomaly_flag = 0
GROUP BY d.year
ORDER BY d.year;

-- C3. Late-delivery rate and lead time by customer state.
SELECT g.state, g.region,
       COUNT(*)                                  AS delivered_orders,
       SUM(f.is_late)                            AS late_orders,
       ROUND(100 * SUM(f.is_late) / COUNT(*), 1) AS late_pct,
       ROUND(AVG(f.total_lead_days), 1)          AS avg_lead_days
FROM fact_order_fulfillment f
JOIN dim_geography g ON f.customer_geo_key = g.geo_key
WHERE f.is_delivered = 1
GROUP BY g.state, g.region
ORDER BY late_pct DESC;

-- C4. Late deliveries by month: is lateness seasonal?
SELECT d.year, d.month,
       COUNT(*)                                  AS delivered_orders,
       ROUND(100 * SUM(f.is_late) / COUNT(*), 1) AS late_pct
FROM fact_order_fulfillment f
JOIN dim_date d ON f.purchase_date_key = d.date_key
WHERE f.is_delivered = 1
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- =====================================================================
-- D. Process: COLLECTING PAYMENT  (fact_payment, transaction fact)
-- =====================================================================

-- D1. Payment type mix.
SELECT t.payment_type, t.description,
       COUNT(*)                                       AS payments,
       SUM(f.payment_value)                           AS total_value,
       ROUND(100 * SUM(f.payment_value) / tot.total, 1) AS pct_of_value,
       ROUND(AVG(f.installments), 1)                  AS avg_installments
FROM fact_payment f
JOIN dim_payment_type t ON f.payment_type_key = t.payment_type_key
CROSS JOIN (SELECT SUM(payment_value) AS total FROM fact_payment) tot
GROUP BY t.payment_type, t.description, tot.total
ORDER BY total_value DESC;

-- D2. Credit-card installment behaviour: do bigger payments get split more?
SELECT CASE WHEN f.installments <= 1  THEN '1 (single)'
            WHEN f.installments <= 3  THEN '2-3'
            WHEN f.installments <= 6  THEN '4-6'
            WHEN f.installments <= 10 THEN '7-10'
            ELSE '11+' END                       AS installment_band,
       COUNT(*)                                  AS payments,
       ROUND(AVG(f.payment_value), 2)            AS avg_payment_value
FROM fact_payment f
JOIN dim_payment_type t ON f.payment_type_key = t.payment_type_key
WHERE t.payment_type = 'credit_card'
GROUP BY CASE WHEN f.installments <= 1  THEN '1 (single)'
              WHEN f.installments <= 3  THEN '2-3'
              WHEN f.installments <= 6  THEN '4-6'
              WHEN f.installments <= 10 THEN '7-10'
              ELSE '11+' END
ORDER BY avg_payment_value;

-- =====================================================================
-- E. Process: CUSTOMER FEEDBACK  (fact_review, transaction fact)
-- =====================================================================

-- E1. Review score distribution and response time.
SELECT f.review_score,
       COUNT(*)                                         AS reviews,
       ROUND(100 * COUNT(*) / tot.n, 1)                 AS pct,
       ROUND(100 * SUM(f.has_comment) / COUNT(*), 1)    AS pct_with_comment,
       ROUND(AVG(f.response_hours), 1)                  AS avg_response_hours
FROM fact_review f
CROSS JOIN (SELECT COUNT(*) AS n FROM fact_review) tot
GROUP BY f.review_score, tot.n
ORDER BY f.review_score;

-- =====================================================================
-- F. DRILL ACROSS: combining two business processes
--    Each fact is first summarised to the shared (conformed) level, then
--    the results are joined - never fact joined straight to fact at
--    different grains.
-- =====================================================================

-- F1. Feedback vs fulfillment: do late deliveries get worse reviews?
--     Both facts have one row per order, joined on the degenerate
--     dimension order_id.
SELECT CASE WHEN ff.is_delivered = 0 THEN 'not delivered'
            WHEN ff.is_late = 1      THEN 'delivered late'
            ELSE 'delivered on time' END            AS delivery_outcome,
       COUNT(*)                                     AS reviewed_orders,
       ROUND(AVG(r.review_score), 2)                AS avg_score,
       ROUND(100 * SUM(CASE WHEN r.review_score = 1 THEN 1 ELSE 0 END)
             / COUNT(*), 1)                         AS pct_one_star
FROM fact_review r
JOIN fact_order_fulfillment ff ON r.order_id = ff.order_id
GROUP BY CASE WHEN ff.is_delivered = 0 THEN 'not delivered'
              WHEN ff.is_late = 1      THEN 'delivered late'
              ELSE 'delivered on time' END
ORDER BY avg_score DESC;

-- F2. Sales vs payments by month, through the conformed date dimension.
--     Each process is summed separately to month, then the two are joined.
SELECT s.year, s.month, s.gross_sales, p.payments,
       p.payments - s.gross_sales AS difference
FROM (SELECT d.year, d.month, SUM(f.gross_amount) AS gross_sales
      FROM fact_order_item f JOIN dim_date d ON f.purchase_date_key = d.date_key
      GROUP BY d.year, d.month) s
JOIN (SELECT d.year, d.month, SUM(f.payment_value) AS payments
      FROM fact_payment f JOIN dim_date d ON f.purchase_date_key = d.date_key
      GROUP BY d.year, d.month) p
  ON s.year = p.year AND s.month = p.month
ORDER BY s.year, s.month;
