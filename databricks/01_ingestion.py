# Databricks notebook source
# MAGIC %md
# MAGIC Databricks notebook source
# MAGIC
# MAGIC
# MAGIC Databricks Fundamentals: Ingestion and Exploration
# MAGIC
# MAGIC
# MAGIC Objective: read the raw source files, inspect and fix the schema, then compute
# MAGIC
# MAGIC
# MAGIC revenue-by-store and top-10-products-by-revenue using both SQL and PySpark

# COMMAND ----------

# 1. Load the orders CSV and inspect the inferred schema
VOLUME_PATH = "/Volumes/training/bronze/raw_files"
 
orders_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/orders.csv")
)
 
orders_raw.printSchema()
display(orders_raw.limit(10))
print("Row count:", orders_raw.count())

# COMMAND ----------

orders_raw.filter(col("updated_ts").isNull()).count()

# COMMAND ----------

from pyspark.sql.functions import col, lit, try_to_timestamp, count, when
 
orders_typed = (
    orders_raw
    .withColumn("order_ts_parsed", try_to_timestamp(col("order_ts"), lit("yyyy-MM-dd HH:mm:ss")))
    .withColumn("updated_ts_parsed", try_to_timestamp(col("updated_ts"), lit("yyyy-MM-dd HH:mm:ss")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
)
 
# Rows where the timestamp failed to parse are the malformed-date rows
bad_dates = orders_typed.filter(col("order_ts_parsed").isNull())
print("Rows with unparseable order_ts:", bad_dates.count())

# COMMAND ----------

#2. Revenue by store — SQL
orders_typed.createOrReplaceTempView("orders_typed")

# COMMAND ----------

SELECT COUNT(*) AS dup_order_id_groups
FROM (
  SELECT order_id
  FROM orders_raw
  GROUP BY order_id
  HAVING COUNT(*) > 1
);

# COMMAND ----------

# 3. Revenue by store — PySpark
from pyspark.sql.functions import sum as _sum, expr 
revenue_by_store = (    
                    orders_typed    
                    .filter(col("quantity") > 0)    
                    .groupBy("store_id")    
                    .agg(_sum(expr("quantity * unit_price")).alias("revenue"))    
                    .orderBy(col("revenue").desc())
                    )
display(revenue_by_store)

# COMMAND ----------

# 4. Join orders to products — top 10 products by revenue
products_ref = products.select("product_id", "product_name", "category")

top_products = (
    orders_typed
    .filter(col("quantity") > 0)
    .filter(col("product_id") != "")
    .join(products_ref, on="product_id", how="inner")
    .groupBy("product_id", "product_name", "category")
    .agg(_sum(expr("quantity * unit_price")).alias("revenue"))
    .orderBy(col("revenue").desc())
    .limit(10)
)
display(top_products)

# COMMAND ----------

# MAGIC %md
# MAGIC SQL vs PySpark
# MAGIC
# MAGIC Both are declarative; the DataFrame API compiles to the same execution plan as SQL, so there's no real performance difference. The actual distinction is how each is expressed: SQL describes a self-contained result in one block, while the DataFrame API expresses a transformation as a sequence of Python objects that can be composed, reused, and built incrementally.
# MAGIC
# MAGIC This showed up directly in the assignment. Revenue-by-store was simplest in SQL- 
# MAGIC SELECT store_id, SUM(quantity * unit_price) AS revenue 
# MAGIC FROM orders_typed
# MAGIC WHERE quantity > 0 
# MAGIC GROUP BY store_id 
# MAGIC ORDER BY revenue DESC 
# MAGIC
# MAGIC is a single, self-contained result with nothing else depending on it, so writing it as one declarative block matched the shape of the question.
# MAGIC
# MAGIC The schema-correction and data-quality work needed PySpark instead, because the results weren't self-contained, 
# MAGIC bad_dates.count() (789 unparseable order_ts rows) fed into a later check, and the four casts needed to be added and inspected one column at a time.
# MAGIC
# MAGIC The clearest example was the duplicate-order-id check: in SQL it needed a nested subquery 
# MAGIC  (GROUP BY ... HAVING COUNT(*) > 1 inside an outer COUNT(*))
# MAGIC  
# MAGIC to express "group, then filter the groups." 
# MAGIC   
# MAGIC In PySpark it stayed flat — 
# MAGIC   .groupBy("order_id").count().filter(col("count") > 1) 
# MAGIC   
# MAGIC because each step is just the next object in a chain, which is exactly what the DataFrame API is built to express.

# COMMAND ----------

