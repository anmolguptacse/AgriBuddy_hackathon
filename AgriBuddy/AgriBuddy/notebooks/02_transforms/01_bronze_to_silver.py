# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze → Silver (Spark Transforms)
# MAGIC Deduplicates, normalises crop names, filters to target mandis,
# MAGIC and writes clean records to the Silver Delta table.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/nirajv@iisc.ac.in/AgriBuddy/AgriBuddy/src")

from croppulse.config import BRONZE_TABLE, SILVER_TABLE, CROP_NAME_MAP, CROPS, MANDIS
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

# COMMAND ----------

# MAGIC %md ## Step 1 — Read Bronze

# COMMAND ----------

bronze_df = spark.read.table(BRONZE_TABLE)
print(f"Bronze rows: {bronze_df.count():,}")
display(bronze_df.limit(5))

# COMMAND ----------

# Check actual values in Bronze table
bronze_df = spark.read.table(BRONZE_TABLE)
print("=== Sample rows ===")
display(bronze_df.limit(5))

print("\n=== Unique date formats ===")
display(bronze_df.select("date").distinct().limit(5))

print("\n=== Unique crop values ===")
display(bronze_df.select("crop").distinct())

print("\n=== Unique mandi values ===")
display(bronze_df.select("mandi").distinct())

# COMMAND ----------

# MAGIC %md ## Step 2 — Normalise crop names

# COMMAND ----------

# Build a Spark map expression to normalise Hindi/regional aliases
crop_map_expr = F.create_map(
    *[item for pair in CROP_NAME_MAP.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
)

silver_df = (
    bronze_df
    # Normalise crop name: lower → lookup → fallback to title-case original
    .withColumn(
        "crop",
        F.coalesce(
            crop_map_expr[F.lower(F.col("crop"))],
            F.initcap(F.col("crop"))
        )
    )
    # Cast date string → DateType
    .withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd").cast(DateType()))
    # Drop nulls on key columns
    .dropna(subset=["date", "crop", "mandi", "modal_price"])
    # Filter to target crops & mandis only
    .filter(F.col("crop").isin(CROPS) & F.col("mandi").isin(MANDIS))
    # Drop negative prices
    .filter(F.col("modal_price") > 0)
)

print(f"Silver rows after cleaning: {silver_df.count():,}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Deduplicate (keep last record per date/crop/mandi)

# COMMAND ----------

from pyspark.sql.window import Window

dedup_window = Window.partitionBy("date", "crop", "mandi").orderBy(F.desc("modal_price"))

silver_df = (
    silver_df
    .withColumn("_rank", F.row_number().over(dedup_window))
    .filter(F.col("_rank") == 1)
    .drop("_rank")
)

print(f"Rows after dedup: {silver_df.count():,}")

# COMMAND ----------

# MAGIC %md ## Step 4 — Write to Silver Delta

# COMMAND ----------

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("crop", "mandi")
    .saveAsTable(SILVER_TABLE)
)

print(f"Silver table written: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Verify

# COMMAND ----------

result = spark.read.table(SILVER_TABLE)
display(result.groupBy("crop", "mandi").agg(
    F.count("*").alias("rows"),
    F.min("date").alias("from_date"),
    F.max("date").alias("to_date"),
    F.avg("modal_price").alias("avg_price"),
).orderBy("crop", "mandi"))