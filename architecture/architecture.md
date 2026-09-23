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
