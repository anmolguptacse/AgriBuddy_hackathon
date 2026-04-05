# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — MLflow Model Registry
# MAGIC Promotes the best Prophet run to the Model Registry
# MAGIC so the app can load it at inference time.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/croppulse/src")

from croppulse.config import MLFLOW_EXPERIMENT_NAME, MODEL_REGISTRY_NAME
import mlflow

client = mlflow.tracking.MlflowClient()

# COMMAND ----------

# MAGIC %md ## Find the best overall run (lowest MAPE)

# COMMAND ----------

exp  = client.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
runs = client.search_runs(
    experiment_ids = [exp.experiment_id],
    order_by       = ["metrics.MAPE ASC"],
    max_results    = 1,
)

if not runs:
    raise RuntimeError("No MLflow runs found. Run 01_prophet_training.py first.")

best_run = runs[0]
print(f"Best run:   {best_run.info.run_id}")
print(f"Crop:       {best_run.data.params.get('crop')}")
print(f"Mandi:      {best_run.data.params.get('mandi')}")
print(f"MAPE:       {best_run.data.metrics.get('MAPE'):.2f}%")

# COMMAND ----------

# MAGIC %md ## Register to Model Registry

# COMMAND ----------

model_uri = f"runs:/{best_run.info.run_id}/prophet_model"

registered = mlflow.register_model(
    model_uri  = model_uri,
    name       = MODEL_REGISTRY_NAME,
)
print(f"Registered: {MODEL_REGISTRY_NAME} v{registered.version}")

# Transition to Production
client.transition_model_version_stage(
    name    = MODEL_REGISTRY_NAME,
    version = registered.version,
    stage   = "Production",
    archive_existing_versions = True,
)
print(f"Transitioned to Production stage.")

# COMMAND ----------

# MAGIC %md ## Verify — load and predict

# COMMAND ----------

import mlflow.prophet, pandas as pd

model = mlflow.prophet.load_model(f"models:/{MODEL_REGISTRY_NAME}/Production")
future   = model.make_future_dataframe(periods=10)
forecast = model.predict(future)

print("\n10-day forecast (₹/quintal):")
display(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(10))