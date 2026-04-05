# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Prophet Price Forecasting + MLflow
# MAGIC Trains a Prophet model per crop per mandi on days 1-60.
# MAGIC Logs all runs to MLflow. Open the MLflow UI during the demo.

# COMMAND ----------

# MAGIC %pip install prophet --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/nirajv@iisc.ac.in/AgriBuddy/AgriBuddy/src")

# ── Force MLflow to use REST API, not Spark ───────────────────────────────────
import os
import mlflow

# On Databricks serverless, get the host and token from environment
DATABRICKS_HOST  = spark.conf.get("spark.databricks.workspaceUrl")
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

os.environ["DATABRICKS_HOST"]  = f"https://{DATABRICKS_HOST}"
os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN

# Set tracking URI to REST endpoint — bypasses Spark config entirely
mlflow.set_tracking_uri(f"databricks")

print(f"Host:  https://{DATABRICKS_HOST}")
print(f"Token: {DATABRICKS_TOKEN[:8]}...")
print(f"MLflow URI: {mlflow.get_tracking_uri()}")

# COMMAND ----------

import requests
import mlflow
import mlflow.prophet
from prophet import Prophet
import pandas as pd, numpy as np

HOST    = "https://dbc-a700adb7-a236.cloud.databricks.com"
TOKEN   = DATABRICKS_TOKEN
headers = {"Authorization": f"Bearer {TOKEN}"}

# Set tracking URI to full HTTPS — no Spark involved
mlflow.set_tracking_uri(HOST)

EXPERIMENT_NAME  = "/Shared/croppulse_prophet"
CROPS            = ["Onion", "Tomato"]
MANDIS           = ["Nashik", "Pune"]
GOLD_TABLE       = "main.croppulse.gold_price_features"
FORECAST_PERIODS = 10
TRAIN_DAYS       = 60

# Create or fetch experiment via REST only — no mlflow.set_experiment()
resp = requests.post(
    f"{HOST}/api/2.0/mlflow/experiments/create",
    headers=headers,
    json={"name": EXPERIMENT_NAME},
)
if "RESOURCE_ALREADY_EXISTS" in resp.text:
    resp2  = requests.get(
        f"{HOST}/api/2.0/mlflow/experiments/get-by-name",
        headers=headers,
        params={"experiment_name": EXPERIMENT_NAME},
    )
    EXP_ID = resp2.json()["experiment"]["experiment_id"]
    print(f"✓ Experiment exists — id={EXP_ID}")
else:
    EXP_ID = resp.json().get("experiment_id")
    print(f"✓ Experiment created — id={EXP_ID}")

print(f"✓ MLflow URI: {mlflow.get_tracking_uri()}")
print(f"✓ EXP_ID ready: {EXP_ID}")

# COMMAND ----------

# import sys
# sys.path.insert(0, "/Workspace/Users/nirajv@iisc.ac.in/AgriBuddy/AgriBuddy/src")

# from croppulse.config import (
#     GOLD_TABLE, MLFLOW_EXPERIMENT_NAME,
#     MODEL_REGISTRY_NAME, FORECAST_PERIODS, CROPS, MANDIS
# )

# import mlflow
# import mlflow.prophet
# import pandas as pd
# import numpy as np
# from prophet import Prophet
# from pyspark.sql import functions as F

# # ── Serverless-compatible MLflow setup ────────────────────────────────────────
# # Do NOT call mlflow.set_tracking_uri("databricks") on serverless
# # It is auto-configured. Just set the experiment name directly.

# mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
# print(f"✓ MLflow experiment set: {MLFLOW_EXPERIMENT_NAME}")
# print(f"✓ MLflow tracking URI: {mlflow.get_tracking_uri()}")

# COMMAND ----------

# MAGIC %md ## Step 1 — Load Gold data

# COMMAND ----------

from pyspark.sql import functions as F

gold_pdf = (
    spark.read.table(GOLD_TABLE)
    .filter(F.col("crop").isin(CROPS) & F.col("mandi").isin(MANDIS))
    .orderBy("date")
    .toPandas()
)
gold_pdf["date"] = pd.to_datetime(gold_pdf["date"])
print(f"✓ Loaded {len(gold_pdf):,} rows")
display(gold_pdf.head(3))

# COMMAND ----------

# MAGIC %md ## Step 2 — Train + evaluate per crop per mandi

# COMMAND ----------

import requests, json, time
import numpy as np, pandas as pd
from prophet import Prophet

HOST    = "https://dbc-a700adb7-a236.cloud.databricks.com"
TOKEN   = DATABRICKS_TOKEN
headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def mlflow_post(endpoint, payload):
    r = requests.post(f"{HOST}/api/2.0/mlflow/{endpoint}", headers=headers, json=payload)
    return r.json()

def mlflow_get(endpoint, params=None):
    r = requests.get(f"{HOST}/api/2.0/mlflow/{endpoint}", headers=headers, params=params)
    return r.json()

results_summary = []

for crop in CROPS:
    for mandi in MANDIS:
        subset = (
            gold_pdf[(gold_pdf["crop"] == crop) & (gold_pdf["mandi"] == mandi)]
            .rename(columns={"date": "ds", "modal_price": "y"})
            [["ds", "y"]].sort_values("ds").reset_index(drop=True)
        )

        if len(subset) < 30:
            print(f"  Skipping {crop}-{mandi}: only {len(subset)} rows")
            continue

        train_df = subset.iloc[:TRAIN_DAYS]
        test_df  = subset.iloc[TRAIN_DAYS:]

        print(f"\nTraining {crop} @ {mandi}...")

        # ── Create MLflow run via REST ────────────────────────────────────────
        run_resp = mlflow_post("runs/create", {
            "experiment_id": EXP_ID,
            "run_name": f"{crop}_{mandi}",
            "start_time": int(time.time() * 1000),
        })
        run_id = run_resp["run"]["info"]["run_id"]
        print(f"  Run created: {run_id[:8]}...")

        # ── Log parameters via REST ───────────────────────────────────────────
        mlflow_post("runs/log-batch", {
            "run_id": run_id,
            "params": [
                {"key": "crop",              "value": crop},
                {"key": "mandi",             "value": mandi},
                {"key": "train_days",        "value": str(TRAIN_DAYS)},
                {"key": "forecast_periods",  "value": str(FORECAST_PERIODS)},
                {"key": "changepoint_prior", "value": "0.05"},
                {"key": "seasonality_mode",  "value": "multiplicative"},
            ]
        })

        # ── Train Prophet ─────────────────────────────────────────────────────
        model = Prophet(
            changepoint_prior_scale = 0.05,
            seasonality_mode        = "multiplicative",
            weekly_seasonality      = True,
            yearly_seasonality      = False,
        )
        model.fit(train_df)

        # ── Evaluate ──────────────────────────────────────────────────────────
        future   = model.make_future_dataframe(periods=len(test_df))
        forecast = model.predict(future)
        pred     = forecast.iloc[TRAIN_DAYS:][["ds","yhat"]].reset_index(drop=True)

        if len(test_df) > 0 and len(pred) > 0:
            n      = min(len(test_df), len(pred))
            actual = test_df["y"].values[:n]
            preds  = pred["yhat"].values[:n]
            mape   = float(np.mean(np.abs((actual - preds) / actual)) * 100)
            mae    = float(np.mean(np.abs(actual - preds)))
        else:
            mape, mae = 999.0, 999.0

        # ── Log metrics via REST ──────────────────────────────────────────────
        mlflow_post("runs/log-batch", {
            "run_id": run_id,
            "metrics": [
                {"key": "MAPE", "value": round(mape, 2), "timestamp": int(time.time()*1000), "step": 0},
                {"key": "MAE",  "value": round(mae,  2), "timestamp": int(time.time()*1000), "step": 0},
            ]
        })

        # ── Save model to Volume, log artifact path ───────────────────────────
        import pickle, os
        model_dir  = f"/Volumes/main/croppulse/croppulse_vol/models/{crop}_{mandi}"
        dbutils.fs.mkdirs(model_dir)
        model_path = f"/Volumes/main/croppulse/croppulse_vol/models/{crop}_{mandi}/prophet_model.pkl"

        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        # Log artifact location as a tag
        mlflow_post("runs/set-tags", {
            "run_id": run_id,
            "tags": [{"key": "model_path", "value": model_path}]
        })

        # ── Close run via REST ────────────────────────────────────────────────
        mlflow_post("runs/update", {
            "run_id": run_id,
            "status": "FINISHED",
            "end_time": int(time.time() * 1000),
        })

        print(f"  ✓ MAPE={mape:.1f}%  MAE=₹{mae:.0f}  model→{model_path}")
        results_summary.append({
            "crop": crop, "mandi": mandi,
            "MAPE": round(mape, 2), "MAE": round(mae, 2),
            "run_id": run_id, "model_path": model_path,
        })

print("\n=== Training Complete ===")
for r in sorted(results_summary, key=lambda x: x["MAPE"]):
    flag = "✓ GOOD" if r["MAPE"] < 20 else "⚠ CHECK"
    print(f"  {r['crop']:8} {r['mandi']:8}  MAPE={r['MAPE']:5.1f}%  MAE=₹{r['MAE']:.0f}  {flag}")

# COMMAND ----------

# results_summary = []

# for crop in CROPS:
#     for mandi in MANDIS:
#         subset = (
#             gold_pdf[(gold_pdf["crop"] == crop) & (gold_pdf["mandi"] == mandi)]
#             .rename(columns={"date": "ds", "modal_price": "y"})
#             [["ds", "y"]]
#             .sort_values("ds")
#             .reset_index(drop=True)
#         )

#         if len(subset) < 30:
#             print(f"  Skipping {crop}-{mandi}: only {len(subset)} rows")
#             continue

#         train_df = subset.iloc[:TRAIN_DAYS]
#         test_df  = subset.iloc[TRAIN_DAYS:]

#         # ── Pass experiment_id explicitly — avoids Spark config lookup ────────
#         with mlflow.start_run(
#             run_name      = f"{crop}_{mandi}",
#             experiment_id = EXP_ID,        # ← this is the fix
#         ):
#             mlflow.log_params({
#                 "crop": crop, "mandi": mandi,
#                 "train_days": TRAIN_DAYS,
#                 "forecast_periods": FORECAST_PERIODS,
#                 "changepoint_prior": 0.05,
#                 "seasonality_mode": "multiplicative",
#             })

#             model = Prophet(
#                 changepoint_prior_scale=0.05,
#                 seasonality_mode="multiplicative",
#                 weekly_seasonality=True,
#                 yearly_seasonality=False,
#             )
#             model.fit(train_df)

#             future   = model.make_future_dataframe(periods=len(test_df))
#             forecast = model.predict(future)
#             pred     = forecast.iloc[TRAIN_DAYS:][["ds","yhat"]].reset_index(drop=True)

#             if len(test_df) > 0 and len(pred) > 0:
#                 n      = min(len(test_df), len(pred))
#                 actual = test_df["y"].values[:n]
#                 preds  = pred["yhat"].values[:n]
#                 mape   = np.mean(np.abs((actual - preds) / actual)) * 100
#                 mae    = np.mean(np.abs(actual - preds))
#             else:
#                 mape, mae = 999.0, 999.0

#             mlflow.log_metrics({"MAPE": round(mape, 2), "MAE": round(mae, 2)})
#             mlflow.prophet.log_model(model, artifact_path="prophet_model")

#             print(f"  ✓ {crop} @ {mandi} → MAPE={mape:.1f}%  MAE=₹{mae:.0f}")
#             results_summary.append({
#                 "crop": crop, "mandi": mandi,
#                 "MAPE": round(mape, 2), "MAE": round(mae, 2)
#             })

# print("\n=== Results ===")
# for r in sorted(results_summary, key=lambda x: x["MAPE"]):
#     print(f"  {r['crop']:8} {r['mandi']:8}  MAPE={r['MAPE']:5.1f}%  MAE=₹{r['MAE']:.0f}")

# COMMAND ----------

# MAGIC %md ## Step 3 — Summary

# COMMAND ----------

print("\n=== Back-test Results ===")
for r in sorted(results_summary, key=lambda x: x["MAPE"]):
    flag = "✓ GOOD" if r["MAPE"] < 15 else "⚠ CHECK"
    print(f"  {r['crop']:8} {r['mandi']:8}  MAPE={r['MAPE']:5.1f}%  MAE=₹{r['MAE']:.0f}  {flag}")

print("\nOpen MLflow UI to view experiment runs →")
print(f"  Experiment: {MLFLOW_EXPERIMENT_NAME}")