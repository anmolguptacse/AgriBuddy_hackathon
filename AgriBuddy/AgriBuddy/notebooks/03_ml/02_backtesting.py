# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Back-test: Predicted vs Actual (Days 61–90)
# MAGIC Produces the accuracy proof shown to judges during the demo.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/croppulse/src")

from croppulse.config import (
    GOLD_TABLE, MLFLOW_EXPERIMENT_NAME, CROPS, MANDIS
)
import mlflow
import mlflow.prophet
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

TRAIN_DAYS = 60

# COMMAND ----------

# MAGIC %md ## Load best MLflow run per crop+mandi

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
exp    = client.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
runs   = client.search_runs(
    experiment_ids = [exp.experiment_id],
    order_by       = ["metrics.MAPE ASC"],
)

print(f"Found {len(runs)} MLflow runs")

# COMMAND ----------

# MAGIC %md ## Predicted vs Actual table

# COMMAND ----------

gold_pdf = (
    spark.read.table(GOLD_TABLE)
    .orderBy("date")
    .toPandas()
)
gold_pdf["date"] = pd.to_datetime(gold_pdf["date"])

summary_rows = []

for crop in CROPS:
    for mandi in MANDIS:
        # Find best run for this crop+mandi
        best_run = next(
            (r for r in runs
             if r.data.params.get("crop") == crop
             and r.data.params.get("mandi") == mandi),
            None
        )
        if best_run is None:
            continue

        model_uri = f"runs:/{best_run.info.run_id}/prophet_model"
        model     = mlflow.prophet.load_model(model_uri)

        subset = (
            gold_pdf[(gold_pdf["crop"] == crop) & (gold_pdf["mandi"] == mandi)]
            .rename(columns={"date": "ds", "modal_price": "y"})
            [["ds", "y"]].sort_values("ds").reset_index(drop=True)
        )

        future   = model.make_future_dataframe(periods=0)
        forecast = model.predict(future)
        preds    = forecast[["ds", "yhat"]].set_index("ds")

        test_slice = subset.iloc[TRAIN_DAYS:]
        for _, row in test_slice.iterrows():
            if row["ds"] in preds.index:
                pred_price = preds.loc[row["ds"], "yhat"]
                err_pct    = ((row["y"] - pred_price) / row["y"]) * 100
                summary_rows.append({
                    "Crop": crop, "Mandi": mandi,
                    "Date": str(row["ds"].date()),
                    "Actual (₹)": int(row["y"]),
                    "Predicted (₹)": int(pred_price),
                    "Error %": round(err_pct, 1),
                })

summary_df = pd.DataFrame(summary_rows)
if not summary_df.empty:
    display(summary_df.head(20))

    mape_by_crop = summary_df.groupby(["Crop", "Mandi"]).apply(
        lambda g: round(g["Error %"].abs().mean(), 2)
    ).reset_index(name="MAPE (%)")
    print("\n=== MAPE Summary ===")
    display(mape_by_crop)

# COMMAND ----------

# MAGIC %md ## Visualisation — Predicted vs Actual

# COMMAND ----------

fig, axes = plt.subplots(len(CROPS), len(MANDIS), figsize=(12, 5 * len(CROPS)), facecolor="#ffffff")
if len(CROPS) == 1:
    axes = [axes]

for i, crop in enumerate(CROPS):
    for j, mandi in enumerate(MANDIS):
        ax   = axes[i][j] if len(MANDIS) > 1 else axes[i]
        data = summary_df[(summary_df["Crop"] == crop) & (summary_df["Mandi"] == mandi)]
        if data.empty:
            continue
        ax.plot(data["Date"], data["Actual (₹)"],    "o-",  color="#1D9E75", label="Actual",    markersize=4)
        ax.plot(data["Date"], data["Predicted (₹)"], "s--", color="#E24B4A", label="Predicted", markersize=4)
        ax.set_title(f"{crop} — {mandi}", fontsize=11)
        ax.set_xlabel("Date"); ax.set_ylabel("₹/quintal")
        ax.legend(fontsize=8)
        ax.tick_params(axis="x", rotation=45, labelsize=7)
        ax.grid(axis="y", alpha=0.3)

fig.suptitle("Back-test: Days 61–90 — Predicted vs Actual", fontsize=13, y=1.01)
plt.tight_layout()
display(fig)