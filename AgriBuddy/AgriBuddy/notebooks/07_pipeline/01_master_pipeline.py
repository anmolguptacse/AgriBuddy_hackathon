# Databricks notebook source
# MAGIC %md
# MAGIC # CropPulse — Master Pipeline
# MAGIC **Run this single notebook to reproduce the entire data pipeline.**
# MAGIC Judges: this is the one notebook to run from the README.
# MAGIC
# MAGIC Order of execution:
# MAGIC 1. Autoloader → Bronze Delta
# MAGIC 2. Bronze → Silver (Spark transforms)
# MAGIC 3. Silver → Gold (price features)
# MAGIC 4. Prophet training + MLflow
# MAGIC 5. Back-testing
# MAGIC 6. MLflow model registration
# MAGIC 7. PDF chunking → Delta
# MAGIC 8. FAISS embedding + index build

# COMMAND ----------

import time
start_total = time.time()

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

# COMMAND ----------

section("1/8 — Autoloader → Bronze Delta")
t = time.time()
%run /Users/nirajv@iisc.ac.in/AgriBuddy/AgriBuddy/notebooks/01_ingestion/01_autoloader_bronze
print(f"  Done in {time.time()-t:.0f}s")

# COMMAND ----------

section("2/8 — Bronze → Silver")
t = time.time()
%run ./02_transforms/01_bronze_to_silver
print(f"  Done in {time.time()-t:.0f}s")

# COMMAND ----------

section("3/8 — Silver → Gold (price features)")
t = time.time()
%run ./02_transforms/02_price_features
print(f"  Done in {time.time()-t:.0f}s")

# COMMAND ----------

section("4/8 — Prophet training + MLflow")
t = time.time()
%run ./03_ml/01_prophet_training
print(f"  Done in {time.time()-t:.0f}s")

# COMMAND ----------

section("5/8 — Back-testing")
t = time.time()
%run ./03_ml/02_backtesting
print(f"  Done in {time.time()-t:.0f}s")

# COMMAND ----------

section("6/8 — MLflow model registration")
t = time.time()
%run ./03_ml/03_mlflow_register
print(f"  Done in {time.time()-t:.0f}s")

# COMMAND ----------

section("7/8 — ICAR PDF chunking")
t = time.time()
%run ./04_rag/01_pdf_chunking
print(f"  Done in {time.time()-t:.0f}s")

# COMMAND ----------

section("8/8 — FAISS embedding + index")
t = time.time()
%run ./04_rag/02_embed_and_index
print(f"  Done in {time.time()-t:.0f}s")

# COMMAND ----------

# MAGIC %md ## Pipeline Complete

# COMMAND ----------

total = time.time() - start_total
print(f"\n{'='*60}")
print(f"  PIPELINE COMPLETE in {total/60:.1f} minutes")
print(f"{'='*60}")
print("""
Next steps:
  1. Open MLflow UI to verify experiment runs and MAPE scores
  2. Run the app:
       python app/main.py
     or deploy via Databricks Apps (see app.yaml)

Tables created:
  main.croppulse.bronze_mandi_prices
  main.croppulse.silver_mandi_prices
  main.croppulse.gold_price_features
  main.croppulse.icar_chunks

Model registered:
  MLflow Model Registry → croppulse-prophet (Production)

FAISS index:
  /dbfs/FileStore/croppulse/faiss_index/index.faiss
""")