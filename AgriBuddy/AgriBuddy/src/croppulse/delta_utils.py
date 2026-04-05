"""
delta_utils.py — helpers for reading/writing Delta tables.
All Spark operations live here so notebooks stay thin.
"""
from __future__ import annotations

from typing import Optional
from pyspark.sql import DataFrame, SparkSession


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def read_table(table: str, filter_expr: Optional[str] = None) -> DataFrame:
    """Read a Delta table, optionally filtering rows."""
    spark = get_spark()
    df = spark.read.format("delta").table(table)
    if filter_expr:
        df = df.filter(filter_expr)
    return df


def write_table(
    df: DataFrame,
    table: str,
    mode: str = "overwrite",
    partition_cols: Optional[list] = None,
) -> None:
    """Write a DataFrame to a Delta table."""
    writer = df.write.format("delta").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table)
    print(f"[delta_utils] Written {df.count()} rows → {table} (mode={mode})")


def ensure_schema(catalog: str, schema: str) -> None:
    """Create catalog.schema if not present."""
    spark = get_spark()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"[delta_utils] Schema ready: {catalog}.{schema}")


def get_latest_prices(silver_table: str, crops: list, mandis: list) -> dict:
    """
    Return today's (latest date) modal price per crop per mandi
    as a nested dict: {crop: {mandi: price}}.
    """
    spark = get_spark()
    from pyspark.sql.functions import col, max as spark_max

    df = spark.read.format("delta").table(silver_table)

    latest_date = df.agg(spark_max("date")).collect()[0][0]
    today_df = df.filter(col("date") == latest_date)

    result: dict = {}
    rows = today_df.filter(
        col("crop").isin(crops) & col("mandi").isin(mandis)
    ).collect()

    for row in rows:
        result.setdefault(row["crop"], {})[row["mandi"]] = row["modal_price"]

    return result
