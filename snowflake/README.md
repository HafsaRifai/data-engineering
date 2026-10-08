# Snowflake Pipeline

## Database / schema layout
- Database: `TRAINING`
- Schemas: `RAW` (source as loaded), `CLEAN` (validated/typed), `GOLD`
  (business-ready aggregates)
- Compute: `TRAINING_WH`, X-Small, `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`
- Governance: `analyst_role`, `SELECT` on `GOLD` only (`ALL` + `FUTURE` tables),
  no access to `RAW` or `CLEAN`

## Files
- `01_setup.sql` — database, schemas, warehouse, RAW table structures
  (orders, customers, products, stores)
- `02_ingest.sql` — named CSV/JSON file formats, internal stage, COPY INTO for
  all four CSV sources plus the JSON order-events file into a VARIANT column,
  row-count reconciliation, LATERAL FLATTEN for nested line items and status
  events
- `03_transform.sql` — Clean and Gold Dynamic Tables (`clean.orders_clean`,
  `gold.daily_sales`), declarative, auto-refreshed on `TARGET_LAG`
- `04_pipeline.sql` — Stream on `raw.orders`, a Task that runs a MERGE only
  when the stream has new data, upserting into `clean.orders_tracked`
- `05_snowpark_transform.py` — `gold.product_sales` rebuilt in Snowpark Python,
  same result as the SQL-built version, for the Snowpark requirement
- `06_security.sql` — `analyst_role` creation and grants, plus a direct
  FAIL/SUCCEED test proving the role can read Gold but not Raw, including with
  secondary roles disabled
- `07_capstone_outputs.sql` — the three required business outputs not covered
  by `03_transform.sql` (by store/region, by category, by customer segment
  with average order value), a reconstructed rejected-records table
  (`clean.orders_rejected`) with a reason per row, and a reconciliation table
  (`gold.run_reconciliation`) logging source/accepted/rejected/target row
  counts per run

## Setup
1. Create a Snowflake trial account (Enterprise edition recommended for
   Dynamic Tables)
2. Run `01_setup.sql` through `07_capstone_outputs.sql` in order, in Snowsight
3. Upload `orders.csv`, `customers.csv`, `products.csv`, `stores.csv` into
   `@orders_stage` under matching subfolders (`orders/`, `customers/`,
   `products/`, `stores/`) before running `02_ingest.sql`'s `COPY INTO`
   statements
4. Upload `order_events.json` into `@orders_stage/events/` before the JSON
   load step in `02_ingest.sql`

## Execution order
`01_setup.sql` → `02_ingest.sql` → `03_transform.sql` → `04_pipeline.sql` →
`05_snowpark_transform.py` → `06_security.sql` → `07_capstone_outputs.sql`

## Troubleshooting
- **Worksheet session schema drifts silently**: a `CREATE TASK` (or any object
  creation) can succeed while landing in the wrong schema if the worksheet's
  session context changed (e.g. switching tabs). Run
  `SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();` whenever a "successful"
  statement's effects don't show up where expected, rather than assuming the
  object-creation logic itself is broken. All scripts here set
  `USE SCHEMA ...` explicitly right before creating an object, rather than
  relying on whatever context the worksheet happens to be in.
- **Ambiguous or missing results after a join**: select only the needed
  columns from the joined-in table before joining, same pattern as the
  Databricks `unit_price` collision.
- **Mixed date formats across files**: `TRY_TO_TIMESTAMP` with one format
  string silently returns NULL for a different valid format rather than
  erroring — use `COALESCE(TRY_TO_TIMESTAMP(..., fmt1), TRY_TO_TIMESTAMP(...,
  fmt2))` to try multiple formats before concluding a date is genuinely
  invalid.
- **`CREATE TASK` needs `EXECUTE TASK` privilege**: trial accounts sometimes
  require this granted explicitly, even to `ACCOUNTADMIN` —
  `GRANT EXECUTE TASK ON ACCOUNT TO ROLE ACCOUNTADMIN;` before creating a task.
- **A load error from `VALIDATION_MODE = RETURN_ERRORS` doesn't appear in
  `COPY_HISTORY`**: this is expected — validation mode is a dry run, nothing
  is actually loaded, so there's nothing for `COPY_HISTORY` to log.
- **`Column object has no attribute 'sum'` in Snowpark**: use the imported
  `sum` function (aliased, since it shadows Python's built-in) wrapped around
  the expression — `sum_(col("a") * col("b"))` — not `.sum()` as a method
  on a `Column`.
