# Databricks notebook source
import dlt
from pyspark.sql.functions import (
    col, lit, coalesce, try_to_timestamp, expr, sum as _sum
)

VOLUME_PATH = "/Volumes/training/bronze/raw_files"

# ---------- BRONZE ----------

@dlt.table(
    name="bronze_orders",
    comment="Raw orders, ingested as-is with no cleaning"
)
def bronze_orders():
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{VOLUME_PATH}/orders.csv")
    )


# ---------- SILVER ----------

@dlt.table(
    name="silver_orders_clean",
    comment="Cleaned orders: valid dates, positive quantity, non-null product_id, deduplicated"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("positive_quantity", "quantity > 0")
@dlt.expect_or_drop("non_negative_price", "unit_price >= 0")
def silver_orders_clean():
    df = (
        dlt.read("bronze_orders")
        .withColumn(
            "order_ts_parsed",
            coalesce(
                try_to_timestamp(col("order_ts"), lit("yyyy-MM-dd HH:mm:ss")),
                try_to_timestamp(col("order_ts"), lit("M/d/yyyy H:mm"))
            )
        )
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .filter(col("order_ts_parsed").isNotNull())
        .filter(col("product_id").isNotNull())
        .dropDuplicates(["order_id"])
    )
    return df


# ---------- SILVER ENRICHED ----------

@dlt.table(
    name="silver_orders_enriched",
    comment="Orders joined to product and customer reference data"
)
@dlt.expect_or_drop("known_customer", "customer_id != 'C99999'")
def silver_orders_enriched():
    products_ref = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .csv(f"{VOLUME_PATH}/products.csv")
        .select("product_id", "product_name", "category")
    )
    customers_ref = (
        spark.read.option("header", "true").option("inferSchema", "true")
        .csv(f"{VOLUME_PATH}/customers.csv")
        .select("customer_id", "customer_name", "segment")
    )
    return (
        dlt.read("silver_orders_clean")
        .join(products_ref, on="product_id", how="inner")
        .join(customers_ref, on="customer_id", how="inner")
    )


# ---------- GOLD ----------

@dlt.table(name="gold_daily_sales", comment="Revenue by date")
def gold_daily_sales():
    from pyspark.sql.functions import to_date
    return (
        dlt.read("silver_orders_enriched")
        .withColumn("order_date", to_date(col("order_ts")))
        .groupBy("order_date")
        .agg(_sum(expr("quantity * unit_price")).alias("revenue"))
    )

@dlt.table(name="gold_product_sales", comment="Revenue by product")
def gold_product_sales():
    return (
        dlt.read("silver_orders_enriched")
        .groupBy("product_id", "product_name", "category")
        .agg(_sum(expr("quantity * unit_price")).alias("revenue"))
    )

@dlt.table(name="gold_customer_sales", comment="Revenue by customer")
def gold_customer_sales():
    return (
        dlt.read("silver_orders_enriched")
        .groupBy("customer_id", "customer_name", "segment")
        .agg(_sum(expr("quantity * unit_price")).alias("revenue"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Incident note- Day 4 induced failure
# MAGIC
# MAGIC **What was broken:** The `bronze_orders` table's source path in `bronze_silver_gold.py`
# MAGIC was deliberately changed from `orders.csv` to a nonexistent `orders_typo.csv`, simulating
# MAGIC a missing/renamed source file.
# MAGIC
# MAGIC **Observed failure:** The pipeline failed at initialization with
# MAGIC `Path does not exist: dbfs:/Volumes/training/bronze/raw_files/orders_typo.csv`.
# MAGIC Because `silver_orders_clean` and `silver_orders_enriched` both depend on `bronze_orders`
# MAGIC via `dlt.read(...)`, all three tables failed to resolve — Lakeflow's dependency graph
# MAGIC correctly cascaded the failure downstream rather than allowing later stages to run on
# MAGIC missing data. At the Job level, the downstream `Data_Quality` task correctly showed as
# MAGIC Blocked and never executed, since it depends on the pipeline task succeeding.
# MAGIC
# MAGIC **Resolution:** Reverted the path back to `orders.csv`, saved, and reran. The pipeline
# MAGIC rebuilt all 6 tables successfully, and the Job's dependency chain (Ingest_Transform_Aggregate
# MAGIC → Data_Quality) completed end to end without needing to rebuild or reconfigure anything else.
# MAGIC
# MAGIC **Takeaway:** Dependency-aware failure propagation (both at the pipeline table level and
# MAGIC the Job task level) meant a single bad source file was caught immediately and contained —
# MAGIC no partial or silently-incorrect data reached Gold, and no downstream task ran on top of
# MAGIC a failed upstream stage.