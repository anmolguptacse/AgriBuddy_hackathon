# """
# decision_engine.py — deterministic SELL / HOLD logic.
# Combines price trend (from Gold Delta table) + weather signal.
# Output is a structured verdict dict passed to llm_client.py.
# """
# from __future__ import annotations

# import pandas as pd
# from croppulse.config import (
#     GOLD_TABLE,
#     PRICE_FALL_THRESHOLD,
#     PRICE_RISE_THRESHOLD,
#     FORECAST_PERIODS,
# )
# def _sql_query(query: str) -> list[dict]:
#     """Query Delta tables from Databricks Apps using SQL connector."""
#     from databricks import sql as dbsql
#     import os
#     host  = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
#     token = os.getenv("DATABRICKS_TOKEN", "")
#     http_path = os.getenv("DATABRICKS_HTTP_PATH", "")  # add this to app.yaml
#     with dbsql.connect(
#         server_hostname = host,
#         http_path       = http_path,
#         access_token    = token,
#     ) as conn:
#         with conn.cursor() as cursor:
#             cursor.execute(query)
#             cols = [d[0] for d in cursor.description]
#             return [dict(zip(cols, row)) for row in cursor.fetchall()]

# def get_price_trend(crop: str, mandi: str) -> dict:
#     """
#     Pull the latest price features from the Gold Delta table.
#     Returns a dict with 7-day change %, trend label, and today's price.
#     """
#     try:
#         from pyspark.sql import SparkSession
#         from pyspark.sql.functions import col, max as spark_max

#         spark = SparkSession.builder.getOrCreate()
#         df = spark.read.format("delta").table(GOLD_TABLE)
#         df = df.filter((col("crop") == crop) & (col("mandi") == mandi))

#         latest = df.agg(spark_max("date")).collect()[0][0]
#         row = df.filter(col("date") == latest).collect()[0]

#         return {
#             "crop": crop,
#             "mandi": mandi,
#             "date": str(latest),
#             "modal_price": float(row["modal_price"]),
#             "avg_7d": float(row["avg_7d"]),
#             "avg_30d": float(row["avg_30d"]),
#             "pct_change_7d": float(row["pct_change_7d"]),
#             "trend": row["trend"],
#         }
#     except Exception as exc:
#         print(f"[decision_engine] Spark unavailable ({exc}), using mock data")
#         return _mock_trend(crop, mandi)


# def make_verdict(price_trend: dict, weather: dict) -> dict:
#     """
#     Apply rule table:

#     | Trend    | Weather | Decision   | Confidence |
#     |----------|---------|------------|------------|
#     | FALLING  | WET     | SELL NOW   | HIGH       |
#     | FALLING  | DRY     | SELL SOON  | MEDIUM     |
#     | RISING   | DRY     | HOLD       | HIGH       |
#     | RISING   | WET     | HOLD SHORT | MEDIUM     |
#     | STABLE   | *       | MONITOR    | LOW        |
#     """
#     pct   = price_trend["pct_change_7d"]
#     trend = price_trend["trend"]
#     rain  = weather["signal"]

#     if trend == "FALLING" and rain == "WET":
#         decision, confidence = "SELL NOW",   "HIGH"
#         reason = (
#             f"Price has fallen {abs(pct):.1f}% in 7 days "
#             f"and {weather['rain_3d_mm']} mm rain is forecast — "
#             "storage losses likely. Sell immediately."
#         )
#     elif trend == "FALLING" and rain == "DRY":
#         decision, confidence = "SELL SOON",  "MEDIUM"
#         reason = (
#             f"Price is down {abs(pct):.1f}% over 7 days. "
#             "Weather is dry so short-term storage is safe, "
#             "but sell within 3–5 days before further decline."
#         )
#     elif trend == "RISING" and rain == "DRY":
#         decision, confidence = "HOLD",       "HIGH"
#         reason = (
#             f"Price has risen {pct:.1f}% in 7 days and dry "
#             "weather favours continued gains. Hold for now."
#         )
#     elif trend == "RISING" and rain == "WET":
#         decision, confidence = "HOLD SHORT", "MEDIUM"
#         reason = (
#             f"Price is rising ({pct:.1f}% in 7 days) but "
#             f"{weather['rain_3d_mm']} mm rain expected — "
#             "hold 2–3 days maximum, then reassess."
#         )
#     else:
#         decision, confidence = "MONITOR",    "LOW"
#         reason = (
#             "Price is stable. Monitor over the next 3 days "
#             "before deciding to sell or hold."
#         )

#     return {
#         "decision":   decision,
#         "confidence": confidence,
#         "reason_en":  reason,
#         "price_trend": price_trend,
#         "weather":     weather,
#     }


# def _mock_trend(crop: str, mandi: str) -> dict:
#     """Fallback when Spark is unavailable (local dev)."""
#     return {
#         "crop": crop, "mandi": mandi, "date": "2024-10-15",
#         "modal_price": 820.0, "avg_7d": 940.0, "avg_30d": 1050.0,
#         "pct_change_7d": -12.8, "trend": "FALLING",
#     }


# def get_price_trend(crop: str, mandi: str) -> dict:
#     try:
#         rows = _sql_query(f"""
#             SELECT date, modal_price, avg_7d, avg_30d, pct_change_7d, trend
#             FROM main.croppulse.gold_price_features
#             WHERE crop = '{crop}' AND mandi = '{mandi}'
#             ORDER BY date DESC LIMIT 1
#         """)
#         if rows:
#             r = rows[0]
#             return {
#                 "crop": crop, "mandi": mandi,
#                 "date": str(r["date"]),
#                 "modal_price":   float(r["modal_price"]),
#                 "avg_7d":        float(r["avg_7d"]),
#                 "avg_30d":       float(r["avg_30d"]),
#                 "pct_change_7d": float(r["pct_change_7d"]),
#                 "trend":         r["trend"],
#             }
#     except Exception as e:
#         print(f"[decision_engine] SQL query failed: {e}")
#     return _mock_trend(crop, mandi)   # only if SQL fails



from __future__ import annotations
import os

def _sql_query(query: str) -> list[dict]:
    from databricks import sql as dbsql
    host      = os.getenv("DATABRICKS_HOST", "").replace("https://", "")
    token     = os.getenv("DATABRICKS_TOKEN", "")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    with dbsql.connect(
        server_hostname = host,
        http_path       = http_path,
        access_token    = token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            cols = [d[0] for d in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

def get_price_trend(crop: str, mandi: str) -> dict:
    try:
        rows = _sql_query(f"""
            SELECT date, modal_price, avg_7d, avg_30d, pct_change_7d, trend
            FROM main.croppulse.gold_price_features
            WHERE crop = '{crop}' AND mandi = '{mandi}'
            ORDER BY date DESC LIMIT 1
        """)
        if rows:
            r = rows[0]
            return {
                "crop": crop, "mandi": mandi,
                "date":          str(r["date"]),
                "modal_price":   float(r["modal_price"]),
                "avg_7d":        float(r["avg_7d"]),
                "avg_30d":       float(r["avg_30d"]),
                "pct_change_7d": float(r["pct_change_7d"]),
                "trend":         r["trend"],
            }
    except Exception as e:
        print(f"[decision_engine] SQL failed: {e}")
    return _mock_trend(crop, mandi)

def make_verdict(price_trend: dict, weather: dict) -> dict:
    pct   = price_trend["pct_change_7d"]
    trend = price_trend["trend"]
    rain  = weather["signal"]

    if trend == "FALLING" and rain == "WET":
        decision, confidence = "SELL NOW",   "HIGH"
        reason = f"Price fell {abs(pct):.1f}% in 7 days and {weather['rain_3d_mm']}mm rain forecast — storage losses likely."
    elif trend == "FALLING" and rain == "DRY":
        decision, confidence = "SELL SOON",  "MEDIUM"
        reason = f"Price down {abs(pct):.1f}% over 7 days. Dry weather allows short storage — sell within 3-5 days."
    elif trend == "RISING" and rain == "DRY":
        decision, confidence = "HOLD",       "HIGH"
        reason = f"Price up {pct:.1f}% in 7 days. Dry weather favours continued gains."
    elif trend == "RISING" and rain == "WET":
        decision, confidence = "HOLD SHORT", "MEDIUM"
        reason = f"Price rising ({pct:.1f}%) but {weather['rain_3d_mm']}mm rain expected — hold 2-3 days max."
    else:
        decision, confidence = "MONITOR",    "LOW"
        reason = "Price is stable. Monitor over next 3 days before deciding."

    return {
        "decision":    decision,
        "confidence":  confidence,
        "reason_en":   reason,
        "price_trend": price_trend,
        "weather":     weather,
    }

def _mock_trend(crop: str, mandi: str) -> dict:
    DATA = {
        ("Onion",  "Nashik"): {"modal_price": 820,  "avg_7d": 940,  "pct_change_7d": -12.8, "trend": "FALLING"},
        ("Onion",  "Pune"):   {"modal_price": 1100, "avg_7d": 1050, "pct_change_7d":  +4.8, "trend": "RISING"},
        ("Tomato", "Nashik"): {"modal_price": 750,  "avg_7d": 700,  "pct_change_7d":  +7.1, "trend": "RISING"},
        ("Tomato", "Pune"):   {"modal_price": 900,  "avg_7d": 960,  "pct_change_7d":  -6.3, "trend": "FALLING"},
    }
    d = DATA.get((crop, mandi), {"modal_price": 800, "avg_7d": 800, "pct_change_7d": 0.0, "trend": "STABLE"})
    return {
        "crop": crop, "mandi": mandi, "date": "2026-04-05",
        "modal_price":   float(d["modal_price"]),
        "avg_7d":        float(d["avg_7d"]),
        "avg_30d":       float(d["avg_7d"]),
        "pct_change_7d": float(d["pct_change_7d"]),
        "trend":         d["trend"],
    }