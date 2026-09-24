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
| 1   | 2026-09-23 | Loaded orders.csv, corrected schema (order_ts/updated_ts parsing, quantity/unit_price casts), computed revenue by store in SQL and PySpark, joined top 10 products by revenue, ran data quality baseline scan (789 malformed order_ts, negative qty, missing product IDs, unknown customers, duplicate order IDs), documented SQL vs PySpark tradeoffs | db8e60272a9dd4f5ea0beb148bd69df44daeffa9 |
| 2   | 2026-09-24 | Built bronze.orders (raw copy), silver.orders_clean (removed bad dates, negative qty, duplicates, null product IDs), silver.orders_enriched (joined products/customers, excluded unknown-customer rows), gold.daily_sales/product_sales/customer_sales, and demonstrated MERGE (update + insert in one statement). Full reconciliation documented. | e0f789d47d098f90ace0ee8c928fbdd9867f37bb / e777ba9eb86e25bd0587d3b0f9bf998b3224d714 |
