# """
# chart_utils.py — generates the combined past 10 days + 10-day forecast
# price chart returned by the Gradio app.
# """
# from __future__ import annotations

# import io
# from typing import Optional

# import matplotlib
# matplotlib.use("Agg")
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
# import pandas as pd
# from PIL import Image

# from croppulse.config import GOLD_TABLE, SILVER_TABLE

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
        
# # def get_historical_prices(crop: str, mandi: str, days: int = 10) -> pd.DataFrame:
# #     """Pull last `days` of modal prices from Silver Delta table."""
# #     try:
# #         from pyspark.sql import SparkSession
# #         from pyspark.sql.functions import col, desc

# #         spark = SparkSession.builder.getOrCreate()
# #         df = (
# #             spark.read.format("delta").table(SILVER_TABLE)
# #             .filter((col("crop") == crop) & (col("mandi") == mandi))
# #             .orderBy(desc("date"))
# #             .limit(days)
# #             .toPandas()
# #         )
# #         df["date"] = pd.to_datetime(df["date"])
# #         return df.sort_values("date")
# #     except Exception:
# #         # Mock data for local dev
# #         dates = pd.date_range(end=pd.Timestamp.today(), periods=days, freq="D")
# #         prices = [1100, 1080, 1050, 1000, 970, 940, 910, 880, 850, 820][:days]
# #         return pd.DataFrame({"date": dates, "modal_price": prices, "mandi": mandi, "crop": crop})
# def get_historical_prices(crop: str, mandi: str, days: int = 10) -> pd.DataFrame:
#     try:
#         rows = _sql_query(f"""
#             SELECT date, modal_price FROM main.croppulse.silver_mandi_prices
#             WHERE crop = '{crop}' AND mandi = '{mandi}'
#             ORDER BY date DESC LIMIT {days}
#         """)
#         if rows:
#             df = pd.DataFrame(rows)
#             df["date"] = pd.to_datetime(df["date"])
#             return df.sort_values("date")
#     except Exception as e:
#         print(f"[chart_utils] SQL failed: {e}")
#     # fallback mock
#     dates  = pd.date_range(end=pd.Timestamp.today(), periods=days, freq="D")
#     prices = [1100,1080,1050,1000,970,940,910,880,850,820][:days]
#     return pd.DataFrame({"date": dates, "modal_price": prices})

# def get_forecast(crop: str, mandi: str, periods: int = 10) -> pd.DataFrame:
#     """Load Prophet forecast from MLflow registered model."""
#     try:
#         import mlflow
#         from croppulse.config import MODEL_REGISTRY_NAME

#         model = mlflow.prophet.load_model(f"models:/{MODEL_REGISTRY_NAME}/Production")
#         future = model.make_future_dataframe(periods=periods)
#         forecast = model.predict(future)
#         tail = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(periods)
#         tail = tail.rename(columns={"ds": "date", "yhat": "forecast_price"})
#         tail["date"] = pd.to_datetime(tail["date"])
#         return tail
#     except Exception:
#         # Mock forecast
#         start = pd.Timestamp.today() + pd.Timedelta(days=1)
#         dates = pd.date_range(start=start, periods=periods, freq="D")
#         prices = [800, 790, 780, 760, 745, 730, 720, 710, 700, 695][:periods]
#         lower  = [p - 40 for p in prices]
#         upper  = [p + 40 for p in prices]
#         return pd.DataFrame({
#             "date": dates, "forecast_price": prices,
#             "yhat_lower": lower, "yhat_upper": upper
#         })


# # def get_all_mandi_prices(crop: str, mandis: list) -> dict:
# #     """Return today's price for a crop across all mandis."""
# #     try:
# #         from pyspark.sql import SparkSession
# #         from pyspark.sql.functions import col, max as spark_max

# #         spark = SparkSession.builder.getOrCreate()
# #         df = spark.read.format("delta").table(SILVER_TABLE).filter(col("crop") == crop)
# #         latest = df.agg(spark_max("date")).collect()[0][0]
# #         rows = df.filter(col("date") == latest).collect()
# #         return {row["mandi"]: row["modal_price"] for row in rows if row["mandi"] in mandis}
# #     except Exception:
# #         return {"Nashik": 820, "Pune": 1100}
# def get_all_mandi_prices(crop: str, mandis: list) -> dict:
#     try:
#         rows = _sql_query(f"""
#             SELECT mandi, modal_price FROM main.croppulse.silver_mandi_prices
#             WHERE crop = '{crop}'
#             AND date = (SELECT MAX(date) FROM main.croppulse.silver_mandi_prices
#                         WHERE crop = '{crop}')
#         """)
#         if rows:
#             return {r["mandi"]: float(r["modal_price"]) for r in rows
#                     if r["mandi"] in mandis}
#     except Exception as e:
#         print(f"[chart_utils] SQL failed: {e}")
#     return {"Nashik": 820, "Pune": 1100}  # fallback only

# def build_price_chart(
#     crop: str,
#     mandi: str,
#     all_mandis: list,
#     historical_days: int = 10,
#     forecast_days: int = 10,
# ) -> Image.Image:
#     """
#     Build a two-panel matplotlib figure:
#     - Top: past 10 days price (solid) + 10-day forecast (dashed) + confidence band
#     - Bottom: today's price comparison bar chart across all mandis
#     """
#     hist_df  = get_historical_prices(crop, mandi, historical_days)
#     fore_df  = get_forecast(crop, mandi, forecast_days)
#     mandi_prices = get_all_mandi_prices(crop, all_mandis)

#     fig, (ax1, ax2) = plt.subplots(
#         2, 1, figsize=(9, 6),
#         gridspec_kw={"height_ratios": [2, 1]},
#         facecolor="#ffffff",
#     )
#     fig.subplots_adjust(hspace=0.45)

#     # ── Panel 1: historical + forecast ────────────────────────────────────────
#     ax1.plot(
#         hist_df["date"], hist_df["modal_price"],
#         color="#1D9E75", linewidth=2.2, label="Actual price", marker="o", markersize=4,
#     )

#     if not fore_df.empty:
#         # Connect last historical point to first forecast point smoothly
#         connect_dates  = [hist_df["date"].iloc[-1],  fore_df["date"].iloc[0]]
#         connect_prices = [hist_df["modal_price"].iloc[-1], fore_df["forecast_price"].iloc[0]]
#         ax1.plot(connect_dates, connect_prices, color="#E24B4A", linewidth=1.5, linestyle="--")

#         ax1.plot(
#             fore_df["date"], fore_df["forecast_price"],
#             color="#E24B4A", linewidth=2, linestyle="--", label="10-day forecast",
#         )
#         ax1.fill_between(
#             fore_df["date"], fore_df["yhat_lower"], fore_df["yhat_upper"],
#             alpha=0.12, color="#E24B4A",
#         )
#         ax1.axvline(hist_df["date"].iloc[-1], color="#888780", linewidth=1, linestyle=":")

#     ax1.set_title(f"{crop} — {mandi} mandi price (₹/quintal)", fontsize=11, pad=8)
#     ax1.set_ylabel("₹ / quintal", fontsize=9)
#     ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
#     ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
#     ax1.tick_params(axis="both", labelsize=8)
#     ax1.legend(fontsize=8, loc="upper right")
#     ax1.grid(axis="y", alpha=0.25, linewidth=0.5)
#     for spine in ax1.spines.values():
#         spine.set_linewidth(0.5)

#     # ── Panel 2: today's price across all mandis ──────────────────────────────
#     sorted_mandis = sorted(mandi_prices.keys())
#     prices  = [mandi_prices.get(m, 0) for m in sorted_mandis]
#     colors  = ["#1D9E75" if m == mandi else "#B5D4F4" for m in sorted_mandis]

#     bars = ax2.bar(sorted_mandis, prices, color=colors, width=0.45, edgecolor="none")
#     for bar, price in zip(bars, prices):
#         ax2.text(
#             bar.get_x() + bar.get_width() / 2,
#             bar.get_height() + 15,
#             f"₹{int(price)}",
#             ha="center", va="bottom", fontsize=9, fontweight="bold",
#         )

#     ax2.set_title("Today's price — all mandis", fontsize=10, pad=6)
#     ax2.set_ylabel("₹ / quintal", fontsize=9)
#     ax2.tick_params(axis="both", labelsize=9)
#     ax2.set_ylim(0, max(prices) * 1.25 if prices else 1500)
#     ax2.grid(axis="y", alpha=0.2, linewidth=0.5)
#     for spine in ax2.spines.values():
#         spine.set_linewidth(0.5)

#     # Convert to PIL image for Gradio
#     buf = io.BytesIO()
#     fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
#     buf.seek(0)
#     plt.close(fig)
#     return Image.open(buf)

from __future__ import annotations
import io, os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from PIL import Image

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

def get_historical_prices(crop: str, mandi: str, days: int = 10) -> pd.DataFrame:
    try:
        rows = _sql_query(f"""
            SELECT date, modal_price FROM main.croppulse.silver_mandi_prices
            WHERE crop='{crop}' AND mandi='{mandi}'
            ORDER BY date DESC LIMIT {days}
        """)
        if rows:
            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values("date")
    except Exception as e:
        print(f"[chart_utils] get_historical_prices failed: {e}")
    dates  = pd.date_range(end=pd.Timestamp.today(), periods=days, freq="D")
    prices = [1100,1080,1050,1000,970,940,910,880,850,820][:days]
    return pd.DataFrame({"date": dates, "modal_price": prices})

def get_forecast(crop: str, mandi: str, periods: int = 10) -> pd.DataFrame:
    try:
        import pickle
        model_path = f"/Volumes/main/croppulse/croppulse_vol/models/{crop}_{mandi}/prophet_model.pkl"
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        future   = model.make_future_dataframe(periods=periods)
        forecast = model.predict(future)
        tail     = forecast[["ds","yhat","yhat_lower","yhat_upper"]].tail(periods)
        tail     = tail.rename(columns={"ds":"date","yhat":"forecast_price"})
        tail["date"] = pd.to_datetime(tail["date"])
        return tail
    except Exception as e:
        print(f"[chart_utils] get_forecast failed: {e}")
    start  = pd.Timestamp.today() + pd.Timedelta(days=1)
    dates  = pd.date_range(start=start, periods=periods, freq="D")
    prices = [800,790,780,760,745,730,720,710,700,695][:periods]
    return pd.DataFrame({
        "date": dates, "forecast_price": prices,
        "yhat_lower": [p-40 for p in prices],
        "yhat_upper": [p+40 for p in prices],
    })

def get_all_mandi_prices(crop: str, mandis: list) -> dict:
    try:
        rows = _sql_query(f"""
            SELECT mandi, modal_price FROM main.croppulse.silver_mandi_prices
            WHERE crop='{crop}'
            AND date=(SELECT MAX(date) FROM main.croppulse.silver_mandi_prices
                      WHERE crop='{crop}')
        """)
        if rows:
            return {r["mandi"]: float(r["modal_price"]) for r in rows if r["mandi"] in mandis}
    except Exception as e:
        print(f"[chart_utils] get_all_mandi_prices failed: {e}")
    return {"Nashik": 820, "Pune": 1100}

def build_price_chart(crop: str, mandi: str, all_mandis: list,
                      historical_days: int = 10, forecast_days: int = 10) -> Image.Image:
    hist_df      = get_historical_prices(crop, mandi, historical_days)
    fore_df      = get_forecast(crop, mandi, forecast_days)
    mandi_prices = get_all_mandi_prices(crop, all_mandis)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 6),
                                    gridspec_kw={"height_ratios": [2, 1]},
                                    facecolor="#ffffff")
    fig.subplots_adjust(hspace=0.45)

    ax1.plot(hist_df["date"], hist_df["modal_price"],
             color="#1D9E75", linewidth=2.2, label="Actual price", marker="o", markersize=4)

    if not fore_df.empty:
        connect_dates  = [hist_df["date"].iloc[-1], fore_df["date"].iloc[0]]
        connect_prices = [hist_df["modal_price"].iloc[-1], fore_df["forecast_price"].iloc[0]]
        ax1.plot(connect_dates, connect_prices, color="#E24B4A", linewidth=1.5, linestyle="--")
        ax1.plot(fore_df["date"], fore_df["forecast_price"],
                 color="#E24B4A", linewidth=2, linestyle="--", label="10-day forecast")
        ax1.fill_between(fore_df["date"], fore_df["yhat_lower"], fore_df["yhat_upper"],
                         alpha=0.12, color="#E24B4A")
        ax1.axvline(hist_df["date"].iloc[-1], color="#888780", linewidth=1, linestyle=":")

    ax1.set_title(f"{crop} — {mandi} mandi price (₹/quintal)", fontsize=11, pad=8)
    ax1.set_ylabel("₹ / quintal", fontsize=9)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    ax1.tick_params(axis="both", labelsize=8)
    ax1.legend(fontsize=8, loc="upper right")
    ax1.grid(axis="y", alpha=0.25, linewidth=0.5)

    sorted_mandis = sorted(mandi_prices.keys())
    prices = [mandi_prices.get(m, 0) for m in sorted_mandis]
    colors = ["#1D9E75" if m == mandi else "#B5D4F4" for m in sorted_mandis]
    bars   = ax2.bar(sorted_mandis, prices, color=colors, width=0.45, edgecolor="none")
    for bar, price in zip(bars, prices):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height()+15,
                 f"₹{int(price)}", ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax2.set_title("Today's price — all mandis", fontsize=10, pad=6)
    ax2.set_ylabel("₹ / quintal", fontsize=9)
    ax2.tick_params(axis="both", labelsize=9)
    ax2.set_ylim(0, max(prices)*1.25 if prices else 1500)
    ax2.grid(axis="y", alpha=0.2, linewidth=0.5)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return Image.open(buf)