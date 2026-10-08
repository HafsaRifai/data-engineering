# Architecture

Same business problem, same source files, built on both platforms: daily order
files and reference extracts for customers, products and stores, processed
incrementally into reliable daily sales reporting.

## Data flow (both platforms)

```
Source files (CSV + JSON)
   |
   v
RAW / BRONZE   ---- raw copy, no cleaning, preserves what was received
   |
   v
CLEAN / SILVER ---- validated, typed, deduplicated, joined to reference data
   |
   +--> rejected records (reason + timestamp, kept for audit)
   |
   v
GOLD           ---- business-ready aggregates: by store/region, by category,
                     by customer segment, plus a reconciliation log per run
```

## Databricks

```
+------------------------------------------------------------+
|  Unity Catalog     governance: catalog/schema/table grants, |
|                     lineage, discovery, audit                |
+------------------------------------------------------------+
|  Lakeflow          declarative pipelines (@dlt.table,        |
|                     expectations) + Jobs (scheduled tasks,   |
|                     dependencies, retries)                   |
+------------------------------------------------------------+
|  Delta Lake         ACID tables, MERGE, schema evolution,    |
|                     time travel                               |
+------------------------------------------------------------+
|  Auto Loader         checkpointed incremental file ingestion |
+------------------------------------------------------------+
```

- **Ingestion**: Auto Loader reads new files from a Unity Catalog volume with a
  checkpoint, so reruns only process genuinely new files (`04_incremental_ingestion.py`).
- **Bronze**: `bronze.orders`, raw copy, no filtering.
- **Silver**: `silver.orders_clean` / `silver.orders_enriched` - drops invalid
  dates, non-positive quantity, null product_id, duplicates; joins to
  products/customers.
- **Rejected records**: `bronze.orders_quarantine`, tagged with a
  `rejection_reasons` column per row.
- **Gold**: `gold.daily_sales`, `gold.product_sales`, `gold.customer_sales`,
  plus the capstone additions `gold.daily_sales_by_store_region`,
  `gold.daily_sales_by_category`, `gold.revenue_by_segment`, and
  `gold.run_reconciliation`.
- **Orchestration**: a Lakeflow Job with dependent tasks
  (`Ingest_Transform_Aggregate` -> `Data_Quality`), the second only running if
  the first succeeds.
- **Governance**: `gold_readers` group, `SELECT` on Gold only, verified with
  `SHOW GRANTS`.

## Snowflake

```
+------------------------------------------------------------+
|  Cloud services      auth, metadata, query optimisation      |
+------------------------------------------------------------+
|  Compute              virtual warehouses, sized independently|
|  (TRAINING_WH)         of storage, auto-suspend/auto-resume  |
+------------------------------------------------------------+
|  Storage               one managed copy of the data          |
+------------------------------------------------------------+
```

- **Ingestion**: named file formats + an internal stage, `COPY INTO` per
  source file, idempotent by default (`02_ingest.sql`).
- **Raw**: `raw.orders`, `raw.customers`, `raw.products`, `raw.stores`, raw
  copy, timestamp columns kept as text to tolerate mixed formats.
- **Clean**: `clean.orders_clean`, a Dynamic Table - declarative, kept fresh
  automatically on `TARGET_LAG` (`03_transform.sql`).
- **Rejected records**: `clean.orders_rejected`, reconstructed with the same
  four validity conditions as `orders_clean`'s WHERE clause, tagged with a
  `rejection_reason` column per row (`07_capstone_outputs.sql`).
- **Gold**: `gold.daily_sales` plus the capstone additions
  `gold.daily_sales_by_store_region`, `gold.daily_sales_by_category`,
  `gold.revenue_by_segment`, and `gold.run_reconciliation`.
- **Incremental change tracking**: a Stream on `raw.orders` + a Task that
  MERGEs changes into `clean.orders_tracked` only when the stream has new
  data (`04_pipeline.sql`).
- **Governance**: `analyst_role`, `SELECT` on Gold only, verified with a
  direct FAIL/SUCCEED test against `raw` vs `gold`.

## Three architectural differences

| # | Area | Databricks | Snowflake |
|---|------|-----------|-----------|
| 1 | Storage and access | Open-format files (Delta/Parquet) in Unity Catalog Volumes, readable by path | Data in Snowflake's own managed storage, reached only through SQL objects |
| 2 | Compute and work style | Spark-based; notebooks with PySpark and SQL; clusters or serverless | SQL-first; virtual warehouses sized in T-shirts, `AUTO_SUSPEND`/`AUTO_RESUME` |
| 3 | Incremental/pipeline tooling | Auto Loader (checkpointed files) + Lakeflow (declarative pipelines, expectations, Jobs) | Streams (row-level change tracking) + Tasks (scheduled SQL) + Dynamic Tables (declarative, `TARGET_LAG`-based) |

## Why this ended up correct

Despite being built independently on each platform, both pipelines land on
the same row-level data quality findings from the same source data: 766
negative-quantity rows, 760 null product_id rows (Databricks numbering) /
the equivalent conditions reconstructed in Snowflake's `orders_rejected`, and
matching Gold revenue totals within rounding. This cross-platform agreement
is itself evidence the transformations are correct, not just that each
platform's code runs without error.
