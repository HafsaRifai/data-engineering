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
| 1   |      |         |        |
