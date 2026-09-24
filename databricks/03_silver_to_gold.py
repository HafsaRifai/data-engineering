# Databricks notebook source
# MAGIC %md
# MAGIC load the source 
# MAGIC

# COMMAND ----------

orders_enriched = spark.table("training.silver.orders_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC gold.daily_sales

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, expr, to_date

daily_sales = (
    orders_enriched
    .withColumn("order_date", to_date(col("order_ts")))
    .groupBy("order_date")
    .agg(_sum(expr("quantity * unit_price")).alias("revenue"))
    .orderBy("order_date")
)

# COMMAND ----------

display(daily_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC gold.product_sales

# COMMAND ----------

product_sales = (
    orders_enriched
    .groupBy("product_id", "product_name", "category")
    .agg(_sum(expr("quantity * unit_price")).alias("revenue"))
    .orderBy(col("revenue").desc())
)

display(product_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC gold.customer_sales
# MAGIC

# COMMAND ----------

customer_sales = (
    orders_enriched
    .groupBy("customer_id", "customer_name", "segment")
    .agg(_sum(expr("quantity * unit_price")).alias("revenue"))
    .orderBy(col("revenue").desc())
)

display(customer_sales)

# COMMAND ----------

daily_sales.write.format("delta").mode("overwrite").saveAsTable("training.gold.daily_sales")
product_sales.write.format("delta").mode("overwrite").saveAsTable("training.gold.product_sales")
customer_sales.write.format("delta").mode("overwrite").saveAsTable("training.gold.customer_sales")

# COMMAND ----------

spark.sql("SHOW TABLES IN training.gold").show()

# COMMAND ----------

from pyspark.sql import Row

existing_order = spark.table("training.bronze.orders").limit(1).collect()[0]
print(existing_order)

# COMMAND ----------

from datetime import datetime

update_row = Row(
    order_id="O0000001",
    order_ts="2026-06-14 21:28:21",
    customer_id="C04413",
    product_id="P0164",
    store_id="ST006",
    quantity=10,                      # changed from 4 → 10
    unit_price=114.44,
    updated_ts=datetime(2026, 9, 24, 12, 0, 0)   # new updated_ts
)

new_row = Row(
    order_id="O9999999",              # doesn't exist yet
    order_ts="2026-09-24 12:00:00",
    customer_id="C04413",
    product_id="P0164",
    store_id="ST006",
    quantity=2,
    unit_price=114.44,
    updated_ts=datetime(2026, 9, 24, 12, 0, 0)
)

incoming_batch = spark.createDataFrame([update_row, new_row])
incoming_batch.createOrReplaceTempView("incoming_batch")

# COMMAND ----------

display(incoming_batch)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO training.bronze.orders AS target
# MAGIC USING incoming_batch AS source
# MAGIC ON target.order_id = source.order_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM training.bronze.orders WHERE order_id IN ('O0000001', 'O9999999')

# COMMAND ----------

print("Bronze count after merge:", spark.table("training.bronze.orders").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver → Gold reconciliation
# MAGIC
# MAGIC | Layer | Row count | Change |
# MAGIC |---|---|---|
# MAGIC | Bronze (`bronze.orders`) | 100,493 | raw copy, no filtering |
# MAGIC | Silver (`silver.orders_clean`) | 97,685 | −2,808 total: 789 rows with unparseable `order_ts` removed, 766 rows with negative quantity removed, 493 duplicate `order_id` rows collapsed to one copy each, 760 rows with null `product_id` removed |
# MAGIC | Silver enriched (`silver.orders_enriched`) | 97,085 | −600: rows with `customer_id = 'C99999'` had no matching reference record, correctly excluded by inner join to `customers` |
# MAGIC | Post-MERGE (`bronze.orders`) | 100,494 | +1 new order inserted, 1 existing order updated, proving Delta's upsert behavior in one statement |

# COMMAND ----------

# MAGIC %md
# MAGIC Working data layers..
# MAGIC
# MAGIC - Bronze preserves the source exactly as received, no cleaning, no filtering, no assumptions. i.e, it's always safe to rebuild Silver and Gold from scratch if downstream logic changes, without needing to re-read the original CSV.
# MAGIC
# MAGIC - Silver guarantees every row has a valid, parseable date, a positive quantity, a non-null product reference, and no duplicate order IDs. but does not guarantee every row has a matching customer, since that's handled at the enrichment step, not the base clean.
# MAGIC
# MAGIC - Silver enriched additionally guarantees every row has a valid, resolvable customer and product record, anything that can't be joined is excluded and counted.
# MAGIC
# MAGIC - Gold guarantees business-ready, pre-aggregated numbers (daily_sales, product_sales, customer_sales) built only from fully validated, enriched data. so any number in Gold can be trusted without needing to know the cleaning steps behind it.

# COMMAND ----------

