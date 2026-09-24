# Databricks notebook source
spark.sql("USE CATALOG training")

# COMMAND ----------

# MAGIC %md
# MAGIC Writing the existing orders_raw DataFrame as a managed Delta table. No cleaning just raw data
# MAGIC

# COMMAND ----------

orders_raw.write.format("delta").mode("overwrite").saveAsTable("bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC count should match Day 1 (ingestion.py) orders_raw.count() exactly (raw row count, before any filtering)

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM training.bronze.orders").show()

# COMMAND ----------

bronze_orders = spark.table("training.bronze.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC fix the date and type casting

# COMMAND ----------

from pyspark.sql.functions import col, lit, try_to_timestamp

orders_silver = (
    bronze_orders
    .withColumn("order_ts_parsed", try_to_timestamp(col("order_ts"), lit("yyyy-MM-dd HH:mm:ss")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .filter(col("order_ts_parsed").isNotNull())   # droping the 789 malformed-date rows
    .filter(col("quantity") > 0)                   # droping negative-quantity rows
)

# COMMAND ----------

print("Bronze count:", bronze_orders.count())
print("Silver (after date/qty filter) count:", orders_silver.count())

# COMMAND ----------

# Checking for duplicate order_ids
dup_orders = orders_silver.groupBy("order_id").count().filter(col("count") > 1)
print("Duplicate order_id groups:", dup_orders.count())

# Checking product_id formatting if there's any inconsistency 
orders_silver.select("product_id").distinct().orderBy("product_id").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC why they're duplicated?  is it the same order repeated identically (true duplicate, drop extras), or the same order_id with different updated_ts/values (an update event, keep the latest)?

# COMMAND ----------

dup_ids = dup_orders.select("order_id")
orders_silver.join(dup_ids, on="order_id", how="inner").orderBy("order_id").show(15, truncate=False)

# COMMAND ----------

orders_silver.filter(col("product_id").isNull()).count()
orders_silver.filter(col("product_id") == "").count()

# COMMAND ----------

# MAGIC %md
# MAGIC drop the redundant copy

# COMMAND ----------

orders_silver = orders_silver.dropDuplicates(["order_id"])

# COMMAND ----------

orders_silver.groupBy("order_id").count().filter(col("count") > 1).count()

# COMMAND ----------

print("Null product_id:", orders_silver.filter(col("product_id").isNull()).count())
print("Empty-string product_id:", orders_silver.filter(col("product_id") == "").count())

# COMMAND ----------

orders_silver = orders_silver.filter(col("product_id").isNotNull())

# COMMAND ----------

print("Silver count after dedup + null product_id filter:", orders_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC delta table

# COMMAND ----------

orders_silver.write.format("delta").mode("overwrite").saveAsTable("training.silver.orders_clean")

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM training.silver.orders_clean").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze → Silver:
# MAGIC - Bronze (raw): 100,493 rows
# MAGIC - Dropped: 789 rows with unparseable order_ts (malformed dates)
# MAGIC - Dropped: 766 rows with negative quantity
# MAGIC - Dropped: 493 duplicate order_id rows (exact duplicates, kept one copy each)
# MAGIC - Dropped: 760 rows with null product_id (can't join to product reference)
# MAGIC - Silver (clean): 97,685 rows

# COMMAND ----------

# MAGIC %md
# MAGIC loading products and customer table

# COMMAND ----------

VOLUME_PATH = "/Volumes/training/bronze/raw_files"

products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/products.csv")
)

customers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/customers.csv")
)

# COMMAND ----------

print(products.columns)
print(customers.columns)

# COMMAND ----------

products_ref = products.select("product_id", "product_name", "category")
customers_ref = customers.select("customer_id", "customer_name", "segment", "city")

# COMMAND ----------

orders_enriched = (
    orders_silver
    .join(products_ref, on="product_id", how="inner")
    .join(customers_ref, on="customer_id", how="inner")
)

print("Silver count before enrichment:", orders_silver.count())
print("Enriched count after joins:", orders_enriched.count())

# COMMAND ----------

# MAGIC %md
# MAGIC finding out whether it's product_id or customer_id causing the mismatch

# COMMAND ----------

# Rows that failed to match products
no_product_match = orders_silver.join(products_ref, on="product_id", how="left_anti")
print("Rows with no matching product:", no_product_match.count())

# Rows that failed to match customers
no_customer_match = orders_silver.join(customers_ref, on="customer_id", how="left_anti")
print("Rows with no matching customer:", no_customer_match.count())

# COMMAND ----------

no_customer_match.select("customer_id").distinct().show(20, truncate=False)

# COMMAND ----------

orders_silver.filter(col("customer_id") == "C99999").count()

# COMMAND ----------

# MAGIC %md
# MAGIC Silver → Enriched reconciliation:
# MAGIC - Silver (clean): 97,685 rows
# MAGIC - Dropped: 600 rows with customer_id = 'C99999'
# MAGIC - Enriched: 97,085 rows

# COMMAND ----------

orders_enriched.write.format("delta").mode("overwrite").saveAsTable("training.silver.orders_enriched")

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM training.silver.orders_enriched").show()

# COMMAND ----------

