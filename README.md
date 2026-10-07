# Data Engineering Training — Databricks & Snowflake

Two-week intern training project: the same retail pipeline (orders, customers,
products, stores) implemented on both Databricks and Snowflake, following the
medallion / raw-clean-gold pattern.

## Structure
- `dataset/` — source CSV/JSON files (orders, customers, products, stores, order_events)
- `databricks/` — Bronze/Silver/Gold notebooks and pipeline code
- `snowflake/` — setup, ingestion, transform and pipeline SQL
- `architecture/` — architecture diagram and notes

## Setup
1. Databricks: see `databricks/README.md`
2. Snowflake: see `snowflake/README.md`

## Execution order
1. Load dataset into raw/bronze layer
2. Clean into silver
3. Aggregate into gold
4. Schedule incremental runs
5. Run reconciliation checks

## Daily log
| Day | Date | Summary | Commit |
|-----|------|---------|--------|
| 1   | 2026-09-23 | Loaded orders.csv, corrected schema (order_ts/updated_ts parsing, quantity/unit_price casts), computed revenue by store in SQL and PySpark, joined top 10 products by revenue, ran data quality baseline scan (789 malformed order_ts, negative qty, missing product IDs, unknown customers, duplicate order IDs), documented SQL vs PySpark tradeoffs | `db8e60272a9dd4f5ea0beb148bd69df44daeffa9` |
| 2   | 2026-09-24 | Built bronze.orders (raw copy), silver.orders_clean (removed bad dates, negative qty, duplicates, null product IDs), silver.orders_enriched (joined products/customers, excluded unknown-customer rows), gold.daily_sales/product_sales/customer_sales, and demonstrated MERGE (update + insert in one statement). Full reconciliation documented. | `e0f789d47d098f90ace0ee8c928fbdd9867f37bb` / `e777ba9eb86e25bd0587d3b0f9bf998b3224d714` |
| 3   | 2026-09-25 | Built training.bronze.orders_incremental via Auto Loader with checkpoint (3 daily files, 945 rows total), proved idempotency on rerun with no duplication, demonstrated controlled schema evolution (discount_code column added via a third file, required explicit rerun + mergeSchema), applied data quality checks (invalid dates, negative quantities, missing product IDs, unknown customers, duplicate order IDs), built orders_quarantine (641 flagged rows with reasons) and orders_incremental_clean (604 final rows after recovering pure-duplicates). Full reconciliation documented. |` d76e2fd44c6aa96302c4f3ef39e87b6b733a6094` |
| 4   | 2026-09-25 | Converted Bronze/Silver/Gold logic into a managed Lakeflow Declarative Pipeline (bronze_silver_gold.py) with dlt.expect_or_drop quality checks (non-null order_id, positive quantity, non-negative price, known customer_id); built a Lakeflow Job with two dependent tasks (Ingest_Transform_Aggregate → Data_Quality); deliberately induced a failure (missing source file), observed dependency-cascade failure and task blocking, fixed and reran successfully. Incident note documented. | `039b6d61209cb50e1001d903f3fe685d10264beb` |
| 5   | 2026-09-26 | Created gold_readers group with SELECT-only access on Gold schema; verified zero grants on Silver via SHOW GRANTS, confirming Unity Catalog's default-deny model blocks Silver access entirely; inspected full lineage from gold.product_sales back to bronze.orders and source volume files; reviewed Day 1 top-products query for efficiency (early filtering, narrow column selection before join, appropriately-sized 2X-Small Serverless SQL Warehouse). Mentor presentation and Week 1 checkpoint pending. | `4e350e3f76bd50ced36e6603086e1a1a56d85840` |
| 6   | 2026-09-28 | Set up Snowflake trial; created TRAINING database with RAW/CLEAN/GOLD schemas and TRAINING_WH (X-Small, AUTO_SUSPEND = 60, AUTO_RESUME = TRUE, verified via SHOW WAREHOUSES); created RAW tables for orders, customers, products, stores (timestamp columns kept as VARCHAR to tolerate malformed dates); wrote a repeatable setup script (snowflake/01_setup.sql) and an architecture note covering Snowflake's storage/compute/cloud-services layers and three differences from Databricks. | `a0c0ef80316cd5538c157b6ff73c4e7082bea858`, `383dedbce042f898d7fb99c641fa7d6288de06e6` |
| 7   | 2026-09-29 | Created named CSV/JSON file formats and an internal stage; loaded all four RAW tables via COPY INTO, reconciled row counts (orders: 100,493, matching Databricks); loaded order_events JSON into a VARIANT column and flattened line_items and status_events with LATERAL FLATTEN; triggered a real load error (column-count mismatch) via VALIDATION_MODE = RETURN_ERRORS, documented root cause and recovery (fix source, reload with ON_ERROR = 'CONTINUE'); consolidated into a repeatable ingestion script (02_ingest.sql). | `72085169ab316da26a4e10e1ee80997597140379` |
| 8   | 2026-10-04 | Built Clean (clean.orders_clean) and Gold (gold.daily_sales) Dynamic Tables with automatic TARGET_LAG-based refresh; built a Stream on raw.orders, a target table, and a Task running MERGE only when the stream has new data, tested end to end (order update flowed through automatically); diagnosed and fixed a task silently created in the wrong schema due to stale session context; wrote up Dynamic Tables vs. Streams/Tasks/MERGE trade-offs. Split into 03_transform.sql and 04_pipeline.sql. | `7a791ab0a292633addce861c91f5447ddb626e97` `047ecb37d904bed4c88e6fc81afea09563f11c08` |
| 9   | 2026-10-05 | Built gold.product_sales_snowpark via Snowpark Python, matching the SQL-built Gold table's output; created a least-privilege analyst_role with SELECT-only access on Gold (ALL + FUTURE tables), verified via SHOW GRANTS and a direct FAIL/SUCCEED test (raw.products denied, gold table allowed) including with secondary roles disabled; inspected Query Profile for the product_sales build (join/aggregate/sort operators, 100% cache hit, 0.34MB scanned). Warehouse-size cost comparison and the one-page operational review still in progress. | `e39ef7f1bd32647b11a1473eed38ab4bc682e8dc` `6fed1b55fc8f46c3aa501fc8b4f3984a03cefd94` |

