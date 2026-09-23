# Snowflake Pipeline

## Object layout
- Database: `TRAINING`
- Schemas: `RAW`, `CLEAN`, `GOLD`
- Warehouse: small, AUTO_SUSPEND enabled

## Files
- `01_setup.sql` — Day 6: database/schema/warehouse creation, raw table DDL
- `02_ingestion.sql` — Day 7: stages, file formats, COPY INTO, JSON/VARIANT loading
- `03_transform.sql` — Day 8: Dynamic Tables / Streams and Tasks, Clean and Gold layers
- `04_pipeline.sql` — Day 8-9: task graph / MERGE orchestration, Snowpark hooks

## Setup
1. Run `01_setup.sql` against a training account
2. Upload `dataset/` files to a named internal stage
3. Run `02_ingestion.sql`, then `03_transform.sql`, then `04_pipeline.sql`

## Troubleshooting
- If COPY INTO reports rejected rows, check VALIDATION_MODE output before adjusting the file format
- If a Dynamic Table does not refresh, check its TARGET_LAG and upstream table staleness
