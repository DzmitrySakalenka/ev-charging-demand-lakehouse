# Databricks notebook source
# Copyright (c) 2026 Dzmitry Sakalenka.
# ruff: noqa: N999
# Purpose: Load Open-Meteo hourly weather into a Bronze Delta table.

import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "ev_charging_demo"
BRONZE_SCHEMA = "bronze"
TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.openmeteo_weather_hourly_raw"

RAW_WEATHER_CSV_PATH = "/Volumes/ev_charging_demo/bronze/raw_files/openmeteo_weather/meteo.csv"
SOURCE_URL = "https://open-meteo.com/en/docs/historical-weather-api"
BATCH_ID = F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")

# Open-Meteo's CSV export has a two-row location/units metadata block
# (lines 0-1), a blank separator (line 2), the hourly time-series header
# (line 3), then the hourly observations (lines 4+). We tag every line with
# its index, pull the header out of line 3, and parse the remaining
# non-empty lines with from_csv. This keeps the metadata block out of the
# Bronze table while preserving every observation row verbatim. The metadata
# itself is recoverable via the `source_file` column if needed downstream.
HOURLY_HEADER_LINE_IDX = 3


def _sanitize(name: str) -> str:
    """Normalize a source column name to a Delta-safe snake_case identifier."""
    return re.sub(r"_+", "_", re.sub(r"[^0-9a-z]+", "_", name.lower())).strip("_")


# `monotonically_increasing_id()` is sequential within a partition. The CSV
# is a few MB and reads as a single Spark partition, so the IDs match file
# order. If this file ever exceeds `spark.sql.files.maxPartitionBytes`
# (default 128 MB), revisit this with a `Window.orderBy` numbering instead.
text_df = spark.read.text(RAW_WEATHER_CSV_PATH).withColumn("line_idx", F.monotonically_increasing_id())

header_text = text_df.filter(F.col("line_idx") == HOURLY_HEADER_LINE_IDX).select("value").first()["value"]

raw_columns = [_sanitize(name) for name in header_text.split(",")]
csv_schema = ", ".join(f"{name} string" for name in raw_columns)

raw_df = (
    text_df
    .filter(F.col("line_idx") > HOURLY_HEADER_LINE_IDX)
    .filter(F.length(F.trim(F.col("value"))) > 0)
    .select(F.from_csv(F.col("value"), csv_schema).alias("parsed"))
    .select("parsed.*")
)

bronze_df = (
    raw_df
    .withColumn(
        "raw_record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in raw_columns],
            ),
            256,
        ),
    )
    .withColumn("source_system", F.lit("openmeteo"))
    .withColumn("source_url", F.lit(SOURCE_URL))
    .withColumn("source_file", F.lit(RAW_WEATHER_CSV_PATH))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("batch_id", BATCH_ID)
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)

print(f"Created table: {TABLE_NAME}")
print(f"Rows: {bronze_df.count()}")
bronze_df.printSchema()
display(bronze_df.limit(20))  # noqa: F821 (Databricks notebook builtin)
