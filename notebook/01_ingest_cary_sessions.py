# Databricks notebook source
# Copyright (c) 2026 Dzmitry Sakalenka.
# ruff: noqa: N999
# Purpose: Load Town of Cary EV charging sessions into a Bronze Delta table.

import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "ev_charging_demo"
BRONZE_SCHEMA = "bronze"
TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.cary_charging_sessions_raw"

RAW_CARY_CSV_PATH = "/Volumes/ev_charging_demo/bronze/raw_files/cary_sessions/electric-vehicle-charging-stations.csv"
SOURCE_URL = "https://data.townofcary.org/explore/dataset/electric-vehicle-charging-stations/"
BATCH_ID = F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")

# Town of Cary's CSV starts with a UTF-8 BOM and double-quotes the
# "Station Name" header. Spark strips the BOM when encoding is set
# explicitly; the surviving stray quotes are handled below by sanitizing
# column names before the data reaches Delta. Bronze keeps every column as
# a string so malformed values are preserved verbatim for auditing.
raw_df = (
    spark.read
    .option("header", True)
    .option("encoding", "UTF-8")
    .option("multiLine", True)
    .option("inferSchema", False)
    .csv(RAW_CARY_CSV_PATH)
)


def _sanitize(name: str) -> str:
    """Normalize a source column name to a Delta-safe snake_case identifier."""
    return re.sub(r"_+", "_", re.sub(r"[^0-9a-z]+", "_", name.lower())).strip("_")


# Delta rejects column names containing spaces, slashes, parentheses, etc.
# without column mapping enabled. Renaming up front keeps the Bronze table
# directly queryable and gives Silver/Gold transforms stable identifiers.
renamed_df = raw_df.toDF(*(_sanitize(c) for c in raw_df.columns))
raw_cols = renamed_df.columns

bronze_df = (
    renamed_df
    .withColumn(
        "raw_record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in raw_cols],
            ),
            256,
        ),
    )
    .withColumn("source_system", F.lit("cary_open_data"))
    .withColumn("source_url", F.lit(SOURCE_URL))
    .withColumn("source_file", F.lit(RAW_CARY_CSV_PATH))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("batch_id", BATCH_ID)
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)

print(f"Created table: {TABLE_NAME}")
print(f"Rows: {bronze_df.count()}")
bronze_df.printSchema()
display(bronze_df.limit(20))  # noqa: F821 (Databricks notebook builtin)
