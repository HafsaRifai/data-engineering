# Databricks Pipeline

## Catalog / Schema layout
- Catalog: `training`
- Schemas: `bronze`, `silver`, `gold`, plus `lakeflow` (destination schema for the
  Day 4 Lakeflow Declarative Pipeline's tables, kept separate from the
  manually-built `bronze`/`silver`/`gold` tables from Days 1-3)
- Governance: `gold_readers` group, SELECT + USE SCHEMA on `gold` only, no access
  to `silver` or `bronze` (Day 5)

## Files
- `01_ingestion.py` — Day 1: load raw CSVs, inspect/fix inferred schema, revenue by
  store (SQL + PySpark), top 10 products by revenue, data quality baseline scan
- `02_bronze_to_silver.py` — Day 2: `bronze.orders` raw copy, `silver.orders_clean`
  (dedup, date/quantity/product-id validation), `silver.orders_enriched` (joined to
  products/customers)
- `03_silver_to_gold.py` — Day 2: `gold.daily_sales`, `gold.product_sales`,
  `gold.customer_sales`; MERGE demonstration (update + insert in one statement)
- `04_incremental_ingestion.py` — Day 3: Auto Loader with checkpoint, incremental
  daily-file processing, idempotency proof, controlled schema evolution
  (`discount_code` column), `bronze.orders_quarantine` (rejected rows with reason),
  `silver.orders_incremental_clean` (recovers pure-duplicate rows)
- `bronze_silver_gold_pipeline/` — Day 4: Lakeflow Declarative Pipeline source
  (`bronze_silver_gold.py`), converting Bronze/Silver/Gold into `@dlt.table`
  definitions with `@dlt.expect_or_drop` data quality checks
- `data_quality_check.py` — Day 4: independent verification notebook run as the
  second task in the Lakeflow Job, re-checks the pipeline's output and fails the
  job if a check that should have been caught wasn't

## Setup
1. Confirm Unity Catalog is enabled; `training` catalog and `bronze`/`silver`/`gold`
   schemas exist (create `lakeflow` as well if running the Day 4 pipeline)
2. Upload `dataset/` files to a Unity Catalog volume (e.g.
   `training.bronze.raw_files`)
3. Attach `01_ingestion.py` through `04_incremental_ingestion.py` to a serverless
   or small cluster and run top to bottom
4. For the Lakeflow pipeline: Workflows → Pipelines → Create Pipeline, source =
   `bronze_silver_gold.py`, destination catalog/schema = `training`/`lakeflow`
5. For the Lakeflow Job: two tasks, `Ingest_Transform_Aggregate` (Pipeline type,
   pointing at the pipeline above) → `Data_Quality` (Notebook type, pointing at
   `data_quality_check.py`), with the second depending on the first

## Execution order
`01_ingestion.py` → `02_bronze_to_silver.py` → `03_silver_to_gold.py` →
`04_incremental_ingestion.py` → Lakeflow pipeline/job (Day 4) → governance grants
(Day 5)

## Troubleshooting
- **New notebook session has no variables**: each notebook/session reset requires
  reloading source data from the volume (or `spark.table(...)` from an
  already-written Delta table) — variables don't carry over across sessions.
- **`saveAsTable` fails with "schema cannot be found"**: check the active catalog
  with the volume path (`/Volumes/training/...` implies catalog `training`) — if
  the session's default catalog is `workspace`, run `USE CATALOG training;` first
  or fully qualify table names (`training.bronze.orders`).
- **Ambiguous column reference after a join** (e.g. `unit_price` exists in both
  `orders` and `products`): select only the needed columns from the right-hand
  table before joining, rather than joining the full table.
- **Auto Loader reprocesses files / duplicates rows**: confirm the checkpoint
  location is stable across runs and wasn't cleared; if it was cleared
  intentionally (e.g. to fix a type/schema issue), expect a full reprocess of
  every file in the source folder, not just new ones.
- **Auto Loader fails with `UNKNOWN_FIELD_EXCEPTION` on a new column**: this is
  expected schema-evolution behavior, not a bug — rerun the exact same cell
  unchanged once Auto Loader has recorded the new schema; if it still fails,
  recreate the `readStream` object (not just the write), since the read side
  caches the schema at creation time.
- **Delta write fails with `DELTA_METADATA_MISMATCH` after a schema change**:
  add `.option("mergeSchema", "true")` to the writeStream — Auto Loader's
  `cloudFiles.schemaLocation` only handles read-side evolution, the Delta table
  itself needs separate permission to accept a new column.
- **Mixed date formats across source files break strict type inference**: avoid
  `cloudFiles.inferColumnTypes` forcing a strict TIMESTAMP cast on a column with
  inconsistent formats; keep it as string at Bronze and parse explicitly with
  `coalesce(try_to_timestamp(..., fmt1), try_to_timestamp(..., fmt2))` in Silver.
- **A row flagged as duplicate in quarantine isn't necessarily disposable**: check
  whether a duplicate-flagged row has any other validity issue before deciding to
  recover it — rows that are *only* duplicated can be deduplicated back into the
  clean set; rows that are duplicated *and* otherwise invalid should stay
  quarantined.
