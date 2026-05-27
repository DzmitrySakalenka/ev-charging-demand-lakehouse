# Databricks notebook source
# Copyright (c) 2026 Dzmitry Sakalenka.
# ruff: noqa: N999
"""Load Open-Meteo hourly weather with Databricks Auto Loader."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "ev_charging_demo"
BRONZE_SCHEMA = "bronze"
TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.openmeteo_weather_hourly_raw_autoloader"

SOURCE_DIR = "/Volumes/ev_charging_demo/bronze/raw_files/openmeteo_weather"
SOURCE_URL = "https://open-meteo.com/en/docs/historical-weather-api"

AUTOLOADER_STATE_DIR = "/Volumes/ev_charging_demo/bronze/raw_files/_autoloader_state/openmeteo_weather"
SCHEMA_LOCATION = f"{AUTOLOADER_STATE_DIR}/schema"
CHECKPOINT_LOCATION = f"{AUTOLOADER_STATE_DIR}/checkpoint"

OBSERVATION_START_LINE_NUMBER = 4
SOURCE_COLUMNS = (
    "time",
    "temperature_2m_c",
    "precipitation_mm",
    "rain_mm",
    "wind_speed_10m_km_h",
)
CSV_SCHEMA = ",".join(f"{column_name} STRING" for column_name in SOURCE_COLUMNS)

files_stream_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.includeExistingFiles", "true")
    .load(SOURCE_DIR)
)

# The Open-Meteo CSV export has a metadata block before the hourly header.
# Auto Loader discovers files incrementally, then Spark parses the file content
# into observation rows while retaining the original file path and line number.
line_stream_df = files_stream_df.select(
    F.col("path").alias("source_file"),
    F.posexplode(F.split(F.decode(F.col("content"), "UTF-8"), r"\r?\n")).alias(
        "line_number",
        "line_text",
    ),
)

raw_stream_df = (
    line_stream_df.filter(F.col("line_number") >= F.lit(OBSERVATION_START_LINE_NUMBER))
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
    .withColumn("source_system", F.lit("openmeteo"))
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
