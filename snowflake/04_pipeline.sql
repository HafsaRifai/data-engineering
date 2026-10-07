USE WAREHOUSE training_wh;
USE DATABASE training;
USE SCHEMA clean;

-- ---------------------------------------------------------------------
-- Stream: tracks inserts/updates/deletes on raw.orders since last
-- consumed by the task below.
-- ---------------------------------------------------------------------
CREATE STREAM IF NOT EXISTS raw.orders_stream
    ON TABLE raw.orders;

-- ---------------------------------------------------------------------
-- Target table the Task upserts into.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.orders_tracked (
    order_id     VARCHAR,
    order_ts     VARCHAR,
    customer_id  VARCHAR,
    product_id   VARCHAR,
    store_id     VARCHAR,
    quantity     INTEGER,
    unit_price   NUMBER(12,2),
    updated_ts   VARCHAR,
    last_synced  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;

-- Dropped and recreated explicitly rather than IF NOT EXISTS: if session
-- context ever drifts to the wrong schema, IF NOT EXISTS would silently
-- do nothing instead of fixing a task stuck in the wrong place.
DROP TASK IF EXISTS clean.merge_orders_task;

USE SCHEMA training.clean;

CREATE TASK merge_orders_task
    WAREHOUSE = training_wh
    SCHEDULE = '5 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('raw.orders_stream')
AS
MERGE INTO clean.orders_tracked AS target
USING raw.orders_stream AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    UPDATE SET
        order_ts = source.order_ts,
        quantity = source.quantity,
        unit_price = source.unit_price,
        updated_ts = source.updated_ts,
        last_synced = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (order_id, order_ts, customer_id, product_id, store_id, quantity, unit_price, updated_ts)
    VALUES (source.order_id, source.order_ts, source.customer_id, source.product_id, source.store_id, source.quantity, source.unit_price, source.updated_ts);

ALTER TASK merge_orders_task RESUME;

-- ---------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------
SHOW TASKS IN SCHEMA training.clean;               -- expect state = started
SELECT SYSTEM$STREAM_HAS_DATA('raw.orders_stream');


