# Databricks Pipeline

## Catalog / Schema layout
- Catalog: `training`
- Schemas: `bronze`, `silver`, `gold`

## Files
- `01_ingestion.py` — Day 1: load raw CSVs, inspect/fix schema, SQL + PySpark exploration
- `02_bronze_to_silver.py` — Day 2-3: Bronze to Silver cleaning, dedup, incremental (Auto Loader)
- `03_silver_to_gold.py` — Day 2/4: Silver to Gold business aggregates, Lakeflow pipeline/job

## Setup
1. Confirm Unity Catalog is enabled; `training` catalog and `bronze`/`silver`/`gold` schemas exist
2. Upload `dataset/` files to a Unity Catalog volume (e.g. `training.bronze.raw_files`)
3. Attach notebooks to a small serverless/job cluster

## Troubleshooting
- If Auto Loader reprocesses files, check the checkpoint location is stable across runs
- If MERGE fails on schema mismatch, confirm the Silver table schema matches the source columns
