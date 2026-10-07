
USE WAREHOUSE training_wh;
USE DATABASE training;
USE SCHEMA clean;


CREATE DYNAMIC TABLE IF NOT EXISTS clean.orders_clean
    TARGET_LAG = '1 minute'
    WAREHOUSE = training_wh
AS
SELECT
    order_id,
    COALESCE(
        TRY_TO_TIMESTAMP(order_ts, 'YYYY-MM-DD HH24:MI:SS'),
        TRY_TO_TIMESTAMP(order_ts, 'MM/DD/YYYY HH24:MI')
    ) AS order_ts_parsed,
    customer_id,
    product_id,
    store_id,
    quantity,
    unit_price,
    updated_ts
FROM raw.orders
WHERE order_id IS NOT NULL
  AND quantity > 0
  AND product_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_ts DESC) = 1;

-- ---------------------------------------------------------------------
-- Gold: daily sales, chained off Clean
--    Longer TARGET_LAG than Clean (5 minutes vs 1 minute): Gold
--    aggregates don't need to be as fresh, so refreshing less often
--    uses less compute.
--    Expected: 92 distinct dates.
-- ---------------------------------------------------------------------
CREATE DYNAMIC TABLE IF NOT EXISTS gold.daily_sales
    TARGET_LAG = '5 minutes'
    WAREHOUSE = training_wh
AS
SELECT
    DATE(order_ts_parsed) AS order_date,
    SUM(quantity * unit_price) AS revenue
FROM clean.orders_clean
GROUP BY DATE(order_ts_parsed);

-- ---------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------
SELECT COUNT(*) FROM clean.orders_clean;   -- expect 98,474
SHOW DYNAMIC TABLES IN SCHEMA training.clean;

SELECT COUNT(*) FROM gold.daily_sales;     -- expect 92
SELECT * FROM gold.daily_sales ORDER BY order_date LIMIT 10;
