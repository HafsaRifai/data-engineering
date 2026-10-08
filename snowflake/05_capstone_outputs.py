# Databricks notebook source

from pyspark.sql.functions import col, sum as _sum, count as _count, avg as _avg, expr

VOLUME_PATH = "/Volumes/training/bronze/raw_files"

orders_enriched = spark.table("training.silver.orders_enriched")

stores = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/stores.csv")
    .select("store_id", "store_name", "region")
)


# Daily sales revenue and order count by store and region

orders_with_store = orders_enriched.join(stores, on="store_id", how="inner")

print("Orders enriched count:", orders_enriched.count())
print("Orders with store count:", orders_with_store.count())  # check join drop

from pyspark.sql.functions import to_date

daily_sales_by_store_region = (
    orders_with_store
    .withColumn("order_date", to_date(col("order_ts")))
    .groupBy("order_date", "store_id", "store_name", "region")
    .agg(
        _sum(expr("quantity * unit_price")).alias("revenue"),
        _count("order_id").alias("order_count"),
    )
    .orderBy("order_date", "store_id")
)

daily_sales_by_store_region.write.format("delta").mode("overwrite") \
    .saveAsTable("training.gold.daily_sales_by_store_region")

# Daily sales revenue and units by product category

daily_sales_by_category = (
    orders_enriched
    .withColumn("order_date", to_date(col("order_ts")))
    .groupBy("order_date", "category")
    .agg(
        _sum(expr("quantity * unit_price")).alias("revenue"),
        _sum("quantity").alias("units"),
    )
    .orderBy("order_date", "category")
)

daily_sales_by_category.write.format("delta").mode("overwrite") \
    .saveAsTable("training.gold.daily_sales_by_category")


# Revenue and average order value by customer segment

revenue_by_segment = (
    orders_enriched
    .groupBy("segment")
    .agg(
        _sum(expr("quantity * unit_price")).alias("revenue"),
        _count("order_id").alias("order_count"),
        _avg(expr("quantity * unit_price")).alias("avg_order_value"),
    )
    .orderBy(col("revenue").desc())
)

revenue_by_segment.write.format("delta").mode("overwrite") \
    .saveAsTable("training.gold.revenue_by_segment")


# MAGIC %md
# MAGIC ## Reconciliation output




from pyspark.sql.functions import lit, current_timestamp

bronze_orders = spark.table("training.bronze.orders")
silver_clean = spark.table("training.silver.orders_clean")
silver_enriched = spark.table("training.silver.orders_enriched")

source_rows = bronze_orders.count()
accepted_rows = silver_enriched.count()
rejected_rows = source_rows - accepted_rows

reconciliation = spark.createDataFrame(
    [(source_rows, silver_clean.count(), accepted_rows, rejected_rows)],
    ["source_rows", "accepted_after_cleaning", "target_rows", "rejected_rows"]
).withColumn("run_ts", current_timestamp())

reconciliation.write.format("delta").mode("append") \
    .saveAsTable("training.gold.run_reconciliation")

display(reconciliation)


spark.sql("SELECT rejection_reasons, COUNT(*) AS n FROM training.bronze.orders_quarantine GROUP BY rejection_reasons ORDER BY n DESC").show(20, truncate=False)
