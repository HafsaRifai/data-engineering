# Databricks notebook source
dbutils.fs.ls("/Volumes/training/bronze/raw_files/incremental")

# COMMAND ----------

# MAGIC %md
# MAGIC Auto loader with checkpoint
# MAGIC

# COMMAND ----------

checkpoint_path = "/Volumes/training/bronze/raw_files/checkpoints/orders_incremental"
schema_location = "/Volumes/training/bronze/raw_files/schema/orders_incremental"

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("header", "true")
    .load("/Volumes/training/bronze/raw_files/incremental")
)

query = (
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("training.bronze.orders_incremental")
)

query.awaitTermination()

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

checkpoint_path = "/Volumes/training/bronze/raw_files/checkpoints/orders_incremental"
schema_location = "/Volumes/training/bronze/raw_files/schema/orders_incremental"

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("header", "true")
    .load("/Volumes/training/bronze/raw_files/incremental")
)

query = (
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("training.bronze.orders_incremental")
)

query.awaitTermination()

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

import pandas as pd

df_day2 = spark.read.option("header", "true").csv("/Volumes/training/bronze/raw_files/incremental/orders_20260902.csv")
df_day2.printSchema()

# COMMAND ----------

spark.table("training.bronze.orders_incremental").printSchema()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS training.bronze.orders_incremental")
dbutils.fs.rm("/Volumes/training/bronze/raw_files/checkpoints/orders_incremental", recurse=True)
dbutils.fs.rm("/Volumes/training/bronze/raw_files/schema/orders_incremental", recurse=True)

# COMMAND ----------

checkpoint_path = "/Volumes/training/bronze/raw_files/checkpoints/orders_incremental"
schema_location = "/Volumes/training/bronze/raw_files/schema/orders_incremental"

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")   
    .option("header", "true")
    .load("/Volumes/training/bronze/raw_files/incremental")
)

query = (
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("training.bronze.orders_incremental")
)
query.awaitTermination()

# COMMAND ----------

spark.table("training.bronze.orders_incremental").printSchema()
spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

dbutils.fs.rm("/Volumes/training/bronze/raw_files/incremental/orders_20260902.csv")

# COMMAND ----------

dbutils.fs.ls("/Volumes/training/bronze/raw_files/incremental")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS training.bronze.orders_incremental")
dbutils.fs.rm("/Volumes/training/bronze/raw_files/checkpoints/orders_incremental", recurse=True)
dbutils.fs.rm("/Volumes/training/bronze/raw_files/schema/orders_incremental", recurse=True)

# COMMAND ----------

checkpoint_path = "/Volumes/training/bronze/raw_files/checkpoints/orders_incremental"
schema_location = "/Volumes/training/bronze/raw_files/schema/orders_incremental"

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load("/Volumes/training/bronze/raw_files/incremental")
)

query = (
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("training.bronze.orders_incremental")
)
query.awaitTermination()

spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

# MAGIC %md
# MAGIC reran after 2nd day file uploaded...

# COMMAND ----------

checkpoint_path = "/Volumes/training/bronze/raw_files/checkpoints/orders_incremental"
schema_location = "/Volumes/training/bronze/raw_files/schema/orders_incremental"

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load("/Volumes/training/bronze/raw_files/incremental")
)

query = (
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("training.bronze.orders_incremental")
)
query.awaitTermination()

spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

dbutils.fs.ls("/Volumes/training/bronze/raw_files/incremental")

# COMMAND ----------

# MAGIC %md
# MAGIC verifying...

# COMMAND ----------


query = (
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("training.bronze.orders_incremental")
)
query.awaitTermination()
spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

# MAGIC %md
# MAGIC new column and controlled schema evolution
# MAGIC

# COMMAND ----------

query = (
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("training.bronze.orders_incremental")
)
query.awaitTermination()
spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

# MAGIC %md
# MAGIC explicit rerun after the error 

# COMMAND ----------

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("header", "true")
    # inferColumnTypes removed — keep Bronze as raw strings, cast properly in Silver
    .load("/Volumes/training/bronze/raw_files/incremental")
)

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS training.bronze.orders_incremental")
dbutils.fs.rm(checkpoint_path, recurse=True)
dbutils.fs.rm(schema_location, recurse=True)

# COMMAND ----------

dbutils.fs.rm(schema_location, recurse=True)

orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load("/Volumes/training/bronze/raw_files/incremental")
)

query = (
    orders_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("training.bronze.orders_incremental")
)
query.awaitTermination()

spark.table("training.bronze.orders_incremental").printSchema()
spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

dbutils.fs.ls("/Volumes/training/bronze/raw_files/incremental")

# COMMAND ----------

dbutils.fs.head("/Volumes/training/bronze/raw_files/incremental/orders_20260903.csv", 500)

# COMMAND ----------

checkpoint_path = "/Volumes/training/bronze/raw_files/checkpoints/orders_incremental"
schema_location = "/Volumes/training/bronze/raw_files/schema/orders_incremental"

# COMMAND ----------

spark.table("training.bronze.orders_incremental").printSchema()
spark.sql("SELECT COUNT(*) FROM training.bronze.orders_incremental").show()

# COMMAND ----------

incremental = spark.table("training.bronze.orders_incremental")

# COMMAND ----------

from pyspark.sql.functions import coalesce, try_to_timestamp, col, lit
from pyspark.sql import Window
from pyspark.sql.functions import count as _count

incremental_checked = (
    incremental
    .withColumn(
        "order_ts_parsed",
        coalesce(
            try_to_timestamp(col("order_ts"), lit("yyyy-MM-dd HH:mm:ss")),
            try_to_timestamp(col("order_ts"), lit("M/d/yyyy H:mm"))
        )
    )
    .withColumn("is_valid_date", col("order_ts_parsed").isNotNull())
    .withColumn("is_valid_quantity", col("quantity") > 0)
    .withColumn("is_valid_product", col("product_id").isNotNull())
    .withColumn("is_valid_customer", col("customer_id") != "C99999")
)

dup_window = Window.partitionBy("order_id")
incremental_checked = incremental_checked.withColumn(
    "is_valid_order_id",
    _count("order_id").over(dup_window) == 1
)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, when

incremental_tagged = incremental_checked.withColumn(
    "rejection_reasons",
    concat_ws(
        ",",
        when(~col("is_valid_date"), lit("invalid_date")),
        when(~col("is_valid_quantity"), lit("invalid_quantity")),
        when(~col("is_valid_product"), lit("missing_product_id")),
        when(~col("is_valid_customer"), lit("unknown_customer")),
        when(~col("is_valid_order_id"), lit("duplicate_order_id"))
    )
)

quarantine = incremental_tagged.filter(col("rejection_reasons") != "")
clean_incremental = incremental_tagged.filter(col("rejection_reasons") == "")

print("Total rows:", incremental_tagged.count())
print("Clean rows:", clean_incremental.count())
print("Quarantined rows:", quarantine.count())

# COMMAND ----------

print("Invalid date:", incremental_checked.filter(~col("is_valid_date")).count())
print("Invalid quantity:", incremental_checked.filter(~col("is_valid_quantity")).count())
print("Missing product_id:", incremental_checked.filter(~col("is_valid_product")).count())
print("Unknown customer:", incremental_checked.filter(~col("is_valid_customer")).count())
print("Duplicate order_id:", incremental_checked.filter(~col("is_valid_order_id")).count())

# COMMAND ----------

dup_ids = incremental_checked.filter(~col("is_valid_order_id")).select("order_id").distinct()
print("Distinct duplicated order_ids:", dup_ids.count())

# Look at a few duplicate groups side by side, including which source file they came from
incremental_checked.join(dup_ids, on="order_id", how="inner") \
    .select("order_id", "order_ts", "quantity", "unit_price", "updated_ts") \
    .orderBy("order_id") \
    .show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC recovery logic 

# COMMAND ----------

recoverable = (
    incremental_tagged
    .filter(col("rejection_reasons") == "duplicate_order_id")
    .dropDuplicates(["order_id"])
)
clean_final = clean_incremental.union(recoverable)

print("Recovered after dedup:", recoverable.count())
print("Final clean count:", clean_final.count())

# COMMAND ----------

# MAGIC %md
# MAGIC quarantine write format
# MAGIC

# COMMAND ----------

quarantine.write.format("delta").mode("overwrite").saveAsTable("training.bronze.orders_quarantine")
clean_final.write.format("delta").mode("overwrite").saveAsTable("training.silver.orders_incremental_clean")

print("Total incremental:", incremental_tagged.count())
print("Quarantined:", spark.table("training.bronze.orders_quarantine").count())
print("Clean (final):", spark.table("training.silver.orders_incremental_clean").count())

# COMMAND ----------

# MAGIC %md
# MAGIC verify ....

# COMMAND ----------

print("Total incremental:", incremental_tagged.count())          # 945
print("Quarantined:", spark.table("training.bronze.orders_quarantine").count())  # 641
print("Clean (final):", spark.table("training.silver.orders_incremental_clean").count())  # 619

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Day 3 reconciliation - incremental ingestion, schema evolution, and quarantine
# MAGIC
# MAGIC **Pipeline summary**
# MAGIC
# MAGIC | Stage | Row count | Notes |
# MAGIC |---|---|---|
# MAGIC | Bronze (`bronze.orders_incremental`) | 945 | Auto Loader, 3 daily files processed incrementally (901: 315, 902: 315, 903: 315), idempotency confirmed on rerun with no new files |
# MAGIC | Quarantined (`bronze.orders_quarantine`) | 641 | All rows failing at least one validity check, kept for audit-  breakdown below |
# MAGIC | Clean, final (`silver.orders_incremental_clean`) | 604 | Rows with no issues (304) plus recovered pure-duplicates (300) |
# MAGIC
# MAGIC **Quarantine breakdown by reason**
# MAGIC
# MAGIC | Reason | Count |
# MAGIC |---|---|
# MAGIC | Invalid date | 8 |
# MAGIC | Negative quantity | 12 |
# MAGIC | Missing product_id | 18 |
# MAGIC | Unknown customer (C99999) | 3 |
# MAGIC | Duplicate order_id | 630 |
# MAGIC
# MAGIC Note: reasons are not mutually exclusive — 15 rows were flagged as both duplicate *and* another genuine issue (e.g. a duplicate pair that also carried the malformed date), so the individual reason counts don't sum cleanly to 641.
# MAGIC
# MAGIC **Why duplicates were unusually high**
# MAGIC
# MAGIC 630 of 641 quarantined rows were flagged for duplicate `order_id`. Investigating this showed 315 distinct orders each appearing exactly twice, with matching `quantity`/`unit_price` and the same underlying date, only reformatted (e.g. `2026-07-24 11:15:29` vs `7/24/2026 11:15`). This traces back to how the day3 test file was constructed: it was derived from day2's existing 315 rows (re-saved via Excel, which also changed the date format) rather than genuinely new records, so those rows legitimately exist under two different daily filenames. This is a property of the test data, not a defect in the duplicate-detection logic — the check correctly caught all 630 rows regardless of the underlying cause.
# MAGIC
# MAGIC **Quarantine-for-audit vs. quarantine-for-deletion**
# MAGIC
# MAGIC Every row that fails a validity check is written to the quarantine table, including both copies of a duplicate pair — this preserves a full audit trail so a reviewer can see exactly what was flagged and why, rather than one row silently vanishing with no context. However, quarantine is not the same as deletion: of the 630 duplicate-flagged rows, 300 had *no other issue* besides being a duplicate, so those were deduplicated (`dropDuplicates`) and one copy of each was recovered into the final clean dataset. The remaining 30 rows (15 duplicate pairs) also failed a second check — e.g. an invalid date — and were kept in quarantine permanently rather than recovered, since being a duplicate wasn't their only problem.
# MAGIC
# MAGIC **Schema evolution**
# MAGIC
# MAGIC Introducing `discount_code` as a new optional column in the day3 file caused Auto Loader to halt the stream on first encounter (`UNKNOWN_FIELD_EXCEPTION`), requiring an explicit rerun to pick up the updated schema — this is intentional, controlled behavior, not a failure. A separate `mergeSchema=true` option was required on the Delta write side to let the target table accept the new column, since Auto Loader's schema evolution (read side) and Delta's schema evolution (write side) are handled independently and neither happens automatically without explicit opt-in.
# MAGIC
# MAGIC **Date format handling**
# MAGIC
# MAGIC Day3's file used `M/d/yyyy H:mm` instead of the original `yyyy-MM-dd HH:mm:ss` format. Initially this caused 315 valid rows to be wrongly flagged as invalid dates. Fixed by using `coalesce()` to try both formats before concluding a date is genuinely unparseable — after the fix, only the 8 deliberately-malformed rows (`2026-13-45 99:99:99`) remained flagged.

# COMMAND ----------

