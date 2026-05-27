# Databricks notebook source
# Copyright (c) 2026 Dzmitry Sakalenka.
# ruff: noqa: N999
"""Load Town of Cary charging sessions with Databricks Auto Loader."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "ev_charging_demo"
BRONZE_SCHEMA = "bronze"
TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.cary_charging_sessions_raw_autoloader"

SOURCE_DIR = "/Volumes/ev_charging_demo/bronze/raw_files/cary_sessions"
SOURCE_URL = "https://data.townofcary.org/explore/dataset/electric-vehicle-charging-stations/"

AUTOLOADER_STATE_DIR = "/Volumes/ev_charging_demo/bronze/raw_files/_autoloader_state/cary_sessions"
SCHEMA_LOCATION = f"{AUTOLOADER_STATE_DIR}/schema"
CHECKPOINT_LOCATION = f"{AUTOLOADER_STATE_DIR}/checkpoint"

SOURCE_COLUMNS = (
    "start_date_time",
    "station_name",
    "charging_time",
    "energy_kwh",
    "address_1",
    "address_2",
    "city",
    "state_province",
    "zip_postal_code",
)
CSV_SCHEMA = ",".join(f"{column_name} STRING" for column_name in SOURCE_COLUMNS)
HEADER_LINE_NUMBER = 0

# The source header contains a UTF-8 BOM and a doubly-quoted "Station Name"
# value, while the Bronze table intentionally uses clean snake_case column
# names. Auto Loader's CSV reader tries to reconcile the provided schema names
# with the source header names, so the clean schema can be filled with nulls.
# Reading discovered files as binary content and parsing data lines by position
# avoids header-name matching while retaining Auto Loader file discovery.
files_stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.includeExistingFiles", "true")
    .load(SOURCE_DIR)
)

line_stream_df = files_stream_df.select(
    F.col("path").alias("source_file"),
    F.posexplode(F.split(F.decode(F.col("content"), "UTF-8"), r"\r?\n")).alias(
        "line_number",
        "line_text",
    ),
)

raw_stream_df = (
    line_stream_df.filter(F.col("line_number") > F.lit(HEADER_LINE_NUMBER))
    .filter(F.length(F.trim(F.col("line_text"))) > 0)
    .select(
        "source_file",
        "line_number",
        F.from_csv(F.col("line_text"), CSV_SCHEMA).alias("parsed"),
    )
    .select(
        "parsed.*",
        "source_file",
        F.col("line_number").alias("source_file_line_number"),
    )
)

# The explicit schema keeps the Bronze table easy to query while preserving all
# source values as strings. Silver remains responsible for timestamps, numeric
# casts, zero-duration handling, and station-name cleanup.
bronze_stream_df = (
    raw_stream_df.withColumn(
        "raw_record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(column_name), F.lit("")) for column_name in SOURCE_COLUMNS],
            ),
            256,
        ),
    )
    .withColumn("source_system", F.lit("cary_open_data"))
    .withColumn("source_url", F.lit(SOURCE_URL))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("batch_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
)

query = (
    bronze_stream_df.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TABLE_NAME)
)

query.awaitTermination()

# COMMAND ----------

display(spark.table(TABLE_NAME).limit(20))  # noqa: F821
