# Databricks notebook source
# MAGIC %md
# MAGIC created a gold_reader group and granted access to read 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG training TO `gold_readers`;
# MAGIC GRANT USE SCHEMA ON SCHEMA training.gold TO `gold_readers`;
# MAGIC GRANT SELECT ON SCHEMA training.gold TO `gold_readers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA training.gold;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA training.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Read-only Gold access — verification
# MAGIC
# MAGIC Created group `gold_readers` and granted:
# MAGIC - USE CATALOG on `training`
# MAGIC - USE SCHEMA + SELECT on `training.gold`
# MAGIC
# MAGIC `SHOW GRANTS ON SCHEMA training.gold` confirms `gold_readers` has SELECT/USE SCHEMA.
# MAGIC `SHOW GRANTS ON SCHEMA training.silver` returns 0 rows for `gold_readers` — no grant
# MAGIC exists, so under Unity Catalog's default-deny model, the group cannot read or modify
# MAGIC Silver tables at all, let alone write to them.

# COMMAND ----------

# MAGIC %md
# MAGIC Efficency review...
# MAGIC

# COMMAND ----------

VOLUME_PATH = "/Volumes/training/bronze/raw_files"

orders_typed = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/orders.csv")
)

products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{VOLUME_PATH}/products.csv")
)

# COMMAND ----------

print(spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown"))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Efficiency review — Day 1 top-products query
# MAGIC
# MAGIC Reviewed the Day 1 `top_products` join (orders → products, grouped by product,
# MAGIC top 10 by revenue) against the four efficiency criteria:
# MAGIC
# MAGIC 1. **Filtering early**: Yes, `.filter(quantity > 0)` and `.filter(product_id != "")`
# MAGIC    are applied before the `.join()`, so the join processes fewer rows rather than
# MAGIC    filtering a wider joined result afterward.
# MAGIC
# MAGIC 2. **Avoiding SELECT \***: Yes — before joining, `products` is narrowed to
# MAGIC    `products_ref = products.select("product_id", "product_name", "category")`,
# MAGIC    dropping `unit_price` and `active_flag` rather than carrying the full table
# MAGIC    into the join (this also fixed the earlier `unit_price` column-collision bug
# MAGIC    from Day 1).
# MAGIC
# MAGIC 3. **Shuffles**: Both `.groupBy()` and `.join()` require a shuffle — rows with
# MAGIC    matching keys (`product_id`) need to be collocated across partitions before
# MAGIC    they can be joined or aggregated together. This is inherent to the operation,
# MAGIC    not something to avoid here; the point of filtering/selecting early is to
# MAGIC    shrink the data volume that gets shuffled, not eliminate the shuffle itself.
# MAGIC
# MAGIC 4. **Compute sizing**: All Day 1-5 SQL work ran on a "Serverless Starter Warehouse"
# MAGIC    (2X-Small, the smallest available size), with 1/1 clusters active — no idle or
# MAGIC    unused scale-out capacity. This is appropriately sized for ~100K-row datasets;
# MAGIC    a larger warehouse size would have been unjustified over-provisioning for this
# MAGIC    data volume, and Serverless's auto-scaling model means this sizing decision
# MAGIC    is revisited automatically rather than requiring manual intervention if the
# MAGIC    workload grows.
# MAGIC

# COMMAND ----------

