USE WAREHOUSE training_wh;
USE DATABASE training;
USE SCHEMA raw;

-- ---------------------------------------------------------------------
-- 1. Named file formats
-- ---------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE;

CREATE FILE FORMAT IF NOT EXISTS json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- ---------------------------------------------------------------------
-- 2. Internal stage
--    Source files are uploaded manually via Snowsight into matching
--    subfolders: orders_stage/orders/, /customers/, /products/,
--    /stores/, /events/
-- ---------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS orders_stage
    FILE_FORMAT = csv_format;

-- Confirm what is actually sitting in the stage before loading
LIST @orders_stage;

-- ---------------------------------------------------------------------
-- 3. Load structured (CSV) sources into RAW
--    ON_ERROR = 'CONTINUE': load every good row, skip only malformed
--    ones, rather than rejecting a whole file for one bad row.
-- ---------------------------------------------------------------------
COPY INTO orders
FROM @orders_stage/orders/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO customers
FROM @orders_stage/customers/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO products
FROM @orders_stage/products/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO stores
FROM @orders_stage/stores/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

-- ---------------------------------------------------------------------
-- 4. Load semi-structured (JSON) source into a VARIANT column
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_events (
    payload    VARIANT,
    loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO order_events (payload)
FROM @orders_stage/events/
FILE_FORMAT = (FORMAT_NAME = 'json_format')
ON_ERROR = 'CONTINUE';

-- ---------------------------------------------------------------------
-- 5. Reconciliation - row counts per table
--    Expected: orders 100,493 | customers 5,000 | products 200 |
--    stores 20 | order_events 500
-- ---------------------------------------------------------------------
SELECT 'orders' AS tbl, COUNT(*) AS n FROM orders
UNION ALL SELECT 'customers', COUNT(*) FROM customers
UNION ALL SELECT 'products',  COUNT(*) FROM products
UNION ALL SELECT 'stores',    COUNT(*) FROM stores
UNION ALL SELECT 'order_events', COUNT(*) FROM order_events;

-- Load history detail, per table, for verification / troubleshooting
SELECT file_name, row_count, row_parsed, error_count
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ORDERS',
    START_TIME => DATEADD(hour, -2, CURRENT_TIMESTAMP())
));

-- ---------------------------------------------------------------------
-- 6. Flatten nested JSON: line items and status events
-- ---------------------------------------------------------------------
SELECT
    payload:order_id::STRING          AS order_id,
    payload:customer_id::STRING       AS customer_id,
    li.value:product_id::STRING       AS product_id,
    li.value:quantity::INTEGER        AS quantity,
    li.value:unit_price::NUMBER(12,2) AS unit_price
FROM order_events,
LATERAL FLATTEN(input => payload:line_items) li;

SELECT
    payload:order_id::STRING  AS order_id,
    se.value:status::STRING   AS status,
    se.value:event_ts::STRING AS event_ts
FROM order_events,
LATERAL FLATTEN(input => payload:status_events) se;