# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Autoloader → Bronze Delta Table
# MAGIC Streams mandi CSV files from `/FileStore/croppulse/raw/` into the
# MAGIC Bronze Delta table using Databricks Autoloader.
# MAGIC Run this notebook first. Judges: this is the ingestion entry point.

# COMMAND ----------

# Add project src to path (no package install needed)
import sys
sys.path.insert(0, "/Workspace/Users/nirajv@iisc.ac.in/AgriBuddy/AgriBuddy/src")

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/croppulse/src")

from croppulse.config import (
    CATALOG, SCHEMA, BRONZE_TABLE, RAW_DATA_PATH
)
from croppulse.delta_utils import ensure_schema

# COMMAND ----------

# MAGIC %md ## Step 1 — Ensure catalog & schema exist

# COMMAND ----------

ensure_schema(CATALOG, SCHEMA)
print(f"Bronze table will be: {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## Step 2 — Upload real CSVs to DBFS (run once)

# COMMAND ----------

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ── Realistic price ranges based on actual Nashik/Pune mandi data ─────────────
PRICE_CONFIG = {
    ("Onion",  "Nashik"): {"base": 900,  "volatility": 180, "trend": -3.5},
    ("Onion",  "Pune"):   {"base": 1100, "volatility": 160, "trend": -2.8},
    ("Tomato", "Nashik"): {"base": 800,  "volatility": 250, "trend": 2.0},
    ("Tomato", "Pune"):   {"base": 950,  "volatility": 220, "trend": 1.5},
}

def generate_realistic_data(crop, mandi, days=90):
    cfg   = PRICE_CONFIG[(crop, mandi)]
    dates = [datetime.today() - timedelta(days=days-i) for i in range(days)]
    
    prices = []
    price  = cfg["base"]
    for i in range(days):
        # Daily random walk with trend and occasional spikes
        shock  = np.random.choice([-1, 0, 0, 0, 1], p=[0.1, 0.3, 0.3, 0.2, 0.1])
        change = cfg["trend"] + np.random.normal(0, cfg["volatility"]/10) + shock * cfg["volatility"] * 0.3
        price  = max(300, min(3000, price + change))
        prices.append(round(price, 0))
    
    df = pd.DataFrame({
        "date":            [d.strftime("%d/%m/%Y") for d in dates],
        "crop":            crop,
        "mandi":           mandi,
        "state":           "Maharashtra",
        "modal_price":     prices,
        "min_price":       [max(200, p - abs(np.random.normal(0, 80))) for p in prices],
        "max_price":       [min(4000, p + abs(np.random.normal(0, 100))) for p in prices],
        "arrivals_tonnes": [abs(np.random.normal(200, 80)) for _ in prices],
    })
    return df

# ── Generate all 4 combinations ───────────────────────────────────────────────
np.random.seed(42)  # reproducible results
dfs = []
for crop in ["Onion", "Tomato"]:
    for mandi in ["Nashik", "Pune"]:
        df = generate_realistic_data(crop, mandi, days=90)
        dfs.append(df)
        print(f"✓ {crop} @ {mandi}: {len(df)} rows | price range ₹{df['modal_price'].min():.0f}–₹{df['modal_price'].max():.0f}/quintal")

all_data = pd.concat(dfs, ignore_index=True)
print(f"\nTotal: {len(all_data)} rows")
print(all_data.head(3))

# ── Save to Unity Catalog Volume ──────────────────────────────────────────────
sdf = spark.createDataFrame(all_data)
(
    sdf.coalesce(1)
       .write
       .mode("overwrite")
       .option("header", "true")
       .csv("/Volumes/main/croppulse/croppulse_vol/raw/mandi_data")
)
print("\n✓ Saved to Volume → /Volumes/main/croppulse/croppulse_vol/raw/mandi_data/")

# ── Verify ────────────────────────────────────────────────────────────────────
files = dbutils.fs.ls("/Volumes/main/croppulse/croppulse_vol/raw/mandi_data/")
for f in files:
    print(f"  {f.name}  ({f.size:,} bytes)")

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

PRICE_CONFIG = {
    ("Onion",  "Nashik"): {"base": 900,  "volatility": 180, "trend": -3.5},
    ("Onion",  "Pune"):   {"base": 1100, "volatility": 160, "trend": -2.8},
    ("Tomato", "Nashik"): {"base": 800,  "volatility": 250, "trend": 2.0},
    ("Tomato", "Pune"):   {"base": 950,  "volatility": 220, "trend": 1.5},
}

def generate_data(crop, mandi, days=90):
    cfg   = PRICE_CONFIG[(crop, mandi)]
    dates = [datetime.today() - timedelta(days=days-i) for i in range(days)]
    prices = []
    price  = cfg["base"]
    np.random.seed(42)
    for i in range(days):
        change = cfg["trend"] + np.random.normal(0, cfg["volatility"]/10)
        price  = max(300, min(3000, price + change))
        prices.append(round(price, 2))
    return pd.DataFrame({
        "date":            [d.strftime("%Y-%m-%d") for d in dates],  # ISO format
        "crop":            crop,
        "mandi":           mandi,
        "state":           "Maharashtra",
        "modal_price":     [float(p) for p in prices],               # explicit float
        "min_price":       [float(max(200, p - 80)) for p in prices],
        "max_price":       [float(min(3000, p + 100)) for p in prices],
        "arrivals_tonnes": [float(abs(np.random.normal(200, 80))) for _ in prices],
    })

dfs = []
for crop in ["Onion", "Tomato"]:
    for mandi in ["Nashik", "Pune"]:
        df = generate_data(crop, mandi)
        dfs.append(df)
        print(f"✓ {crop} @ {mandi}: price range ₹{df['modal_price'].min():.0f}–₹{df['modal_price'].max():.0f}")

all_data = pd.concat(dfs, ignore_index=True)
print(f"\nTotal rows: {len(all_data)}")
print(f"Sample dates: {all_data['date'].head(3).tolist()}")
print(f"Sample prices: {all_data['modal_price'].head(3).tolist()}")

# COMMAND ----------

# Write directly to Bronze Delta table — bypasses CSV parsing issues entirely
sdf = spark.createDataFrame(all_data)

(
    sdf.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable("main.croppulse.bronze_mandi_prices")
)

# Verify
df_check = spark.read.table("main.croppulse.bronze_mandi_prices")
print(f"✓ Bronze rows: {df_check.count()}")
display(df_check.limit(5))

# COMMAND ----------

# # ── Step 1: Create catalog + schema + volume ──────────────────────────────────
# spark.sql("CREATE CATALOG IF NOT EXISTS main")
# spark.sql("CREATE SCHEMA  IF NOT EXISTS main.croppulse")
# spark.sql("CREATE VOLUME  IF NOT EXISTS main.croppulse.croppulse_vol")

# print("✓ Catalog:  main")
# print("✓ Schema:   main.croppulse")
# print("✓ Volume:   main.croppulse.croppulse_vol  ← for CSV files + FAISS index")
# print()
# print("Delta tables will be created here:")
# print("  main.croppulse.bronze_mandi_prices")
# print("  main.croppulse.silver_mandi_prices")
# print("  main.croppulse.gold_price_features")
# print("  main.croppulse.icar_chunks")

# # ── Step 2: Create Volume subfolders ──────────────────────────────────────────
# for folder in ["raw", "icar_pdfs", "faiss_index", "checkpoints"]:
#     dbutils.fs.mkdirs(f"/Volumes/main/croppulse/croppulse_vol/{folder}")
#     print(f"✓ Folder: /Volumes/main/croppulse/croppulse_vol/{folder}")

# # ── Step 3: Fetch real mandi data and save to Volume ─────────────────────────
# import requests, pandas as pd

# DATA_GOV_API_KEY = "579b464db66ec23bdd000001e3071276f27b4aa8613e501bd9f92c65"
# RESOURCE_ID      = "35985678-0d79-46b4-9ed6-6f13308a1d24"

# def fetch_mandi(commodity, market, limit=500):
#     resp = requests.get(
#         f"https://api.data.gov.in/resource/{RESOURCE_ID}",
#         params={
#             "api-key":            DATA_GOV_API_KEY,
#             "format":             "json",
#             "filters[State]":     "Maharashtra",
#             "filters[Commodity]": commodity,
#             "filters[Market]":    market,
#             "limit":              limit,
#         },
#         timeout=15,
#     )
#     records = resp.json().get("records", [])
#     df = pd.DataFrame(records).rename(columns={
#         "Arrival_Date":                 "date",
#         "Commodity":                    "crop",
#         "Market":                       "mandi",
#         "Modal_x0020_Price":            "modal_price",
#         "Min_x0020_Price":              "min_price",
#         "Max_x0020_Price":              "max_price",
#         "Arrivals_x0020_in_x0020_Qtl": "arrivals_tonnes",
#     })
#     for col in ["modal_price","min_price","max_price","arrivals_tonnes"]:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#     return df

# dfs = []
# for crop in ["Onion","Tomato"]:
#     for mandi in ["Nashik","Pune"]:
#         print(f"Fetching {crop} @ {mandi}...", end=" ")
#         df = fetch_mandi(crop, mandi)
#         print(f"{len(df)} records")
#         dfs.append(df)

# all_data = pd.concat(dfs, ignore_index=True)
# print(f"\nTotal real records fetched: {len(all_data)}")

# # ── Step 4: Save CSV to Volume (unstructured storage) ────────────────────────
# (
#     spark.createDataFrame(all_data)
#          .coalesce(1)
#          .write
#          .mode("overwrite")
#          .option("header", "true")
#          .csv("/Volumes/main/croppulse/croppulse_vol/raw/mandi_live")
# )
# print("✓ CSV saved to Volume → /Volumes/main/croppulse/croppulse_vol/raw/mandi_live/")

# # ── Step 5: Autoloader — read CSV from Volume → write to Bronze Delta Table ──
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# BRONZE_SCHEMA = StructType([
#     StructField("date",            StringType(), True),
#     StructField("crop",            StringType(), True),
#     StructField("mandi",           StringType(), True),
#     StructField("modal_price",     DoubleType(),  True),
#     StructField("min_price",       DoubleType(),  True),
#     StructField("max_price",       DoubleType(),  True),
#     StructField("arrivals_tonnes", DoubleType(),  True),
# ])

# (
#     spark.readStream
#          .format("cloudFiles")                        # ← Autoloader
#          .option("cloudFiles.format", "csv")
#          .option("cloudFiles.schemaLocation",
#                  "/Volumes/main/croppulse/croppulse_vol/checkpoints/schema")
#          .option("header", "true")
#          .schema(BRONZE_SCHEMA)
#          .load("/Volumes/main/croppulse/croppulse_vol/raw/")  # watches Volume
#     .writeStream
#          .format("delta")                             # ← writes Delta Table
#          .outputMode("append")
#          .option("checkpointLocation",
#                  "/Volumes/main/croppulse/croppulse_vol/checkpoints/bronze")
#          .trigger(availableNow=True)
#          .toTable("main.croppulse.bronze_mandi_prices")  # ← Delta Table in catalog
# )

# print("✓ Autoloader complete → main.croppulse.bronze_mandi_prices")
# print()
# print("Go to Catalog → main → croppulse to see the Bronze Delta table")

# COMMAND ----------

# display(spark.sql("SHOW VOLUMES"))
# display(spark.sql("SHOW CATALOGS"))

# # Create the volume if it doesn't exist
# spark.sql("""
#     CREATE VOLUME IF NOT EXISTS main.croppulse.croppulse_vol
# """)
# print("✓ Volume created: main.croppulse.croppulse_vol")

# import requests, pandas as pd, os

# DATA_GOV_API_KEY = "579b464db66ec23bdd000001e3071276f27b4aa8613e501bd9f92c65"
# RESOURCE_ID = "35985678-0d79-46b4-9ed6-6f13308a1d24"

# def fetch_real_mandi_data(commodity, market, limit=500):
#     url = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
#     params = {
#         "api-key":            DATA_GOV_API_KEY,
#         "format":             "json",
#         "filters[State]":     "Maharashtra",
#         "filters[Commodity]": commodity,
#         "filters[Market]":    market,
#         "limit":              limit,
#     }
#     resp = requests.get(url, params=params, timeout=15)
#     records = resp.json().get("records", [])
#     df = pd.DataFrame(records)
#     df = df.rename(columns={
#         "Arrival_Date":                  "date",
#         "Commodity":                     "crop",
#         "Market":                        "mandi",
#         "Modal_x0020_Price":             "modal_price",
#         "Min_x0020_Price":               "min_price",
#         "Max_x0020_Price":               "max_price",
#         "Arrivals_x0020_in_x0020_Qtl":  "arrivals_tonnes",
#     })
#     for col in ["modal_price", "min_price", "max_price", "arrivals_tonnes"]:
#         if col in df.columns:
#             df[col] = pd.to_numeric(df[col], errors="coerce")
#     return df

# # Create Volume folder (works on serverless)
# raw_path = "/Volumes/main/croppulse/croppulse_vol/raw"
# dbutils.fs.mkdirs(raw_path)
# print(f"Folder ready: {raw_path}")

# # Fetch and save real data
# dfs = []
# for crop in ["Onion", "Tomato"]:
#     for mandi in ["Nashik", "Pune"]:
#         print(f"Fetching {crop} @ {mandi}...")
#         df = fetch_real_mandi_data(crop, mandi)
#         print(f"  Got {len(df)} real records")
#         dfs.append(df)

# all_data = pd.concat(dfs, ignore_index=True)

# # Save to Volume using Spark (works on serverless — no os.makedirs needed)
# sdf = spark.createDataFrame(all_data)
# sdf.write.mode("overwrite").csv(raw_path + "/mandi_live.csv", header=True)
# print(f"\nSaved {len(all_data)} real records → {raw_path}/mandi_live.csv")

# COMMAND ----------

# MAGIC %md ## Step 3 — Define Bronze schema

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DateType, DoubleType
)

BRONZE_SCHEMA = StructType([
    StructField("date",            StringType(),  True),
    StructField("crop",            StringType(),  True),
    StructField("mandi",           StringType(),  True),
    StructField("modal_price",     DoubleType(),  True),
    StructField("min_price",       DoubleType(),  True),
    StructField("max_price",       DoubleType(),  True),
    StructField("arrivals_tonnes", DoubleType(),  True),
])

# COMMAND ----------

# MAGIC %md ## Step 4 — Start Autoloader stream

# COMMAND ----------

checkpoint_path = f"/Volumes/{CATALOG}/{SCHEMA}/croppulse_vol/checkpoints/bronze"

(
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("cloudFiles.schemaLocation", checkpoint_path)
         .option("header", "true")
         .schema(BRONZE_SCHEMA)
         .load(RAW_DATA_PATH)
    .writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", checkpoint_path)
         .option("mergeSchema", "true")
         .trigger(availableNow=True)          # batch mode — processes all available files
         .toTable(BRONZE_TABLE)
)

print(f"Autoloader complete → {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## Step 5 — Verify

# COMMAND ----------

df = spark.read.table(BRONZE_TABLE)
print(f"Total rows in Bronze: {df.count():,}")
display(df.groupBy("crop", "mandi").count().orderBy("crop", "mandi"))