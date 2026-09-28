# Architecture

## Reference pipeline
CSV/JSON sources -> Raw/Bronze -> Clean/Silver -> Business/Gold -> Scheduled pipeline

## Databricks vs Snowflake mapping
| Layer | Databricks | Snowflake |
|---|---|---|
| Ingestion | Auto Loader | Stage + COPY INTO / Snowpipe |
| Raw | Bronze Delta tables | RAW tables |
| Clean | Silver Delta tables | CLEAN Dynamic Tables |
| Business | Gold Delta tables | GOLD tables/Dynamic Tables |
| Orchestration | Lakeflow Jobs | Tasks / task graph |
| Governance | Unity Catalog | RBAC |

(Diagram to be added once both pipelines are built.)

# Architecture notes

## Snowflake (Day 6)

### The three layers

```
+------------------------------------------------------------+
|  CLOUD SERVICES   auth, security, metadata, query optimiser |
+------------------------------------------------------------+
|  COMPUTE          virtual warehouses (independent, sized,   |
|                   auto-suspend / auto-resume)               |
+------------------------------------------------------------+
|  STORAGE          one central, managed copy of the data     |
+------------------------------------------------------------+
```

- **Storage** holds the data once, managed by Snowflake.
- **Compute** is one or more virtual warehouses. Several warehouses can read the same
  storage at the same time without competing, and each can be resized or suspended
  on its own.
- **Cloud services** handles authentication, metadata and query optimisation. It is not
  something the user provisions.

### Object hierarchy used in this project

```
Account
 └── Database: TRAINING
      ├── Schema: RAW    (orders, customers, products, stores - as received)
      ├── Schema: CLEAN  (validated and typed)
      └── Schema: GOLD   (business-ready aggregates)

Compute: TRAINING_WH - X-Small, AUTO_SUSPEND = 60, AUTO_RESUME = TRUE
```

This mirrors the Databricks layout: `training` catalog with `bronze` / `silver` / `gold`
schemas. RAW / CLEAN / GOLD are the same medallion pattern under Snowflake's naming.

Setup is reproducible from `snowflake/01_setup.sql`.

---

## Three architectural differences: Snowflake vs Databricks

| # | Area | Databricks (as used in Week 1) | Snowflake |
|---|------|-------------------------------|-----------|
| 1 | **How data is stored and accessed** | Data sits as open-format files (Delta/Parquet, CSV in Unity Catalog Volumes). Files can be read directly by path, e.g. `/Volumes/training/bronze/raw_files/orders.csv`. | Data is held in Snowflake's own managed storage and reached through SQL objects (tables, views). Files are only touched through a stage when loading. |
| 2 | **Compute engine and how you work** | Spark-based. Work is done in notebooks with PySpark and SQL; compute is a cluster or serverless compute. | SQL-first. Compute is a virtual warehouse sized in T-shirt sizes (`XSMALL`), controlled with settings such as `AUTO_SUSPEND` and `AUTO_RESUME`. |
| 3 | **Ingestion and pipeline tooling** | Auto Loader (checkpointed incremental file ingestion), Lakeflow Declarative Pipelines with expectations, Lakeflow Jobs for scheduling. | Stages and loading commands for ingestion, with Dynamic Tables and Tasks for transformation and scheduling. *(To be confirmed hands-on in Days 7-9.)* |

**Practical consequence of #1 and #2 in this project:** in Databricks the raw file was read
straight from a volume path in code. In Snowflake the raw layer is a table that has to be
created first (`training.raw.orders`) before any data is loaded into it.
