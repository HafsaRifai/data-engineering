-- snowflake/07_capstone_outputs.sql
-- Capstone: required business outputs, rejected records, reconciliation.


USE WAREHOUSE training_wh;
USE DATABASE training;
USE SCHEMA clean;

-- ---------------------------------------------------------------------
-- Daily sales revenue and order count by store and region
-- ---------------------------------------------------------------------
CREATE DYNAMIC TABLE IF NOT EXISTS gold.daily_sales_by_store_region
    TARGET_LAG = '5 minutes'
    WAREHOUSE = training_wh
AS
SELECT
    DATE(o.order_ts_parsed)       AS order_date,
    s.store_id,
    s.store_name,
    s.region,
    SUM(o.quantity * o.unit_price) AS revenue,
    COUNT(o.order_id)              AS order_count
FROM clean.orders_clean o
JOIN raw.stores s ON o.store_id = s.store_id
GROUP BY DATE(o.order_ts_parsed), s.store_id, s.store_name, s.region;

-- ---------------------------------------------------------------------
-- Daily sales revenue and units by product category
-- ---------------------------------------------------------------------
CREATE DYNAMIC TABLE IF NOT EXISTS gold.daily_sales_by_category
    TARGET_LAG = '5 minutes'
    WAREHOUSE = training_wh
AS
SELECT
    DATE(o.order_ts_parsed) AS order_date,
    p.category,
    SUM(o.quantity * o.unit_price) AS revenue,
    SUM(o.quantity)                AS units
FROM clean.orders_clean o
JOIN raw.products p ON o.product_id = p.product_id
GROUP BY DATE(o.order_ts_parsed), p.category;

-- ---------------------------------------------------------------------
-- Revenue and average order value by customer segment
-- ---------------------------------------------------------------------
CREATE DYNAMIC TABLE IF NOT EXISTS gold.revenue_by_segment
    TARGET_LAG = '5 minutes'
    WAREHOUSE = training_wh
AS
SELECT
    c.segment,
    SUM(o.quantity * o.unit_price)  AS revenue,
    COUNT(o.order_id)               AS order_count,
    AVG(o.quantity * o.unit_price)  AS avg_order_value
FROM clean.orders_clean o
JOIN raw.customers c ON o.customer_id = c.customer_id
GROUP BY c.segment;

-- ---------------------------------------------------------------------
-- Rejected records: every raw.orders row that did NOT make it into
-- clean.orders_clean, with a reason. clean.orders_clean already filters
-- on these four conditions (see 03_transform.sql); this reconstructs
-- the rejection reason per row for the capstone's required dataset.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE clean.orders_rejected AS
SELECT
    o.*,
    ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
        IFF(o.order_id IS NULL, 'missing_order_id', NULL),
        IFF(TRY_CAST(o.quantity AS INT) <= 0, 'invalid_quantity', NULL),
        IFF(o.product_id IS NULL, 'missing_product_id', NULL),
        IFF(
            COALESCE(
                TRY_TO_TIMESTAMP(o.order_ts, 'YYYY-MM-DD HH24:MI:SS'),
                TRY_TO_TIMESTAMP(o.order_ts, 'MM/DD/YYYY HH24:MI')
            ) IS NULL,
            'invalid_date', NULL
        )
    ), ',') AS rejection_reason,
    CURRENT_TIMESTAMP() AS processed_at
FROM raw.orders o
WHERE o.order_id IS NULL
   OR TRY_CAST(o.quantity AS INT) <= 0
   OR o.product_id IS NULL
   OR COALESCE(
        TRY_TO_TIMESTAMP(o.order_ts, 'YYYY-MM-DD HH24:MI:SS'),
        TRY_TO_TIMESTAMP(o.order_ts, 'MM/DD/YYYY HH24:MI')
      ) IS NULL;

SELECT rejection_reason, COUNT(*) AS n
FROM clean.orders_rejected
GROUP BY rejection_reason
ORDER BY n DESC;

-- ---------------------------------------------------------------------
-- Reconciliation: one row per run, source/accepted/rejected/target
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.run_reconciliation (
    run_ts          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_rows     NUMBER,
    accepted_rows   NUMBER,
    rejected_rows   NUMBER,
    target_rows     NUMBER
);

INSERT INTO gold.run_reconciliation (source_rows, accepted_rows, rejected_rows, target_rows)
SELECT
    (SELECT COUNT(*) FROM raw.orders)            AS source_rows,
    (SELECT COUNT(*) FROM clean.orders_clean)    AS accepted_rows,
    (SELECT COUNT(*) FROM clean.orders_rejected) AS rejected_rows,
    (SELECT COUNT(*) FROM clean.orders_clean)    AS target_rows;

SELECT * FROM gold.run_reconciliation ORDER BY run_ts DESC;
