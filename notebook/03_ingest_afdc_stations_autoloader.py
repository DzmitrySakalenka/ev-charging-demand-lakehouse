# Databricks notebook source
# Copyright (c) 2026 Dzmitry Sakalenka.
# ruff: noqa: N999
"""Load AFDC station snapshot files with Databricks Auto Loader."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "ev_charging_demo"
BRONZE_SCHEMA = "bronze"
TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.afdc_ev_stations_raw_autoloader"

SOURCE_DIR = "/Volumes/ev_charging_demo/bronze/raw_files/afdc_stations"
SOURCE_URL = "https://developer.nlr.gov/docs/transportation/alt-fuel-stations-v1/all/"

AUTOLOADER_STATE_DIR = "/Volumes/ev_charging_demo/bronze/raw_files/_autoloader_state/afdc_stations"
SCHEMA_LOCATION = f"{AUTOLOADER_STATE_DIR}/schema"
CHECKPOINT_LOCATION = f"{AUTOLOADER_STATE_DIR}/checkpoint"

# Auto Loader's JSON source can infer the whole `fuel_stations` array as a
# string for multi-line whole-response snapshots, which makes `explode` fail
# with a STRING-vs-ARRAY type error. Use Auto Loader for file discovery only,
# then parse each discovered file's content with an explicitly inferred static
# JSON schema so nested station fields remain ARRAY/STRUCT typed.
PAYLOAD_SCHEMA = spark.read.option("multiLine", "true").json(SOURCE_DIR).schema

files_stream_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.includeExistingFiles", "true")
    .load(SOURCE_DIR)
)

# AFDC snapshots are whole-response JSON documents. Exploding `fuel_stations`
# keeps nested station fields as arrays/structs instead of flattening them into
# strings, which makes the Bronze table replayable and useful for Silver.
payload_stream_df = files_stream_df.select(
    F.col("path").alias("source_file"),
    F.from_json(F.decode(F.col("content"), "UTF-8"), PAYLOAD_SCHEMA).alias("payload"),
)

stations_stream_df = (
    payload_stream_df
    .select(
        "source_file",
        F.explode("payload.fuel_stations").alias("station"),
    )
    .select("station.*", "source_file")
)

station_columns = tuple(column_name for column_name in stations_stream_df.columns if column_name != "source_file")

bronze_stream_df = (
    stations_stream_df
    .withColumn(
        "raw_record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(column_name).cast("string"), F.lit("")) for column_name in station_columns],
            ),
            256,
        ),
    )
    .withColumn("source_system", F.lit("afdc_nlr"))
    .withColumn("source_url", F.lit(SOURCE_URL))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("batch_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
)

query = (
    bronze_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(TABLE_NAME)
)

query.awaitTermination()

# COMMAND ----------

display(spark.table(TABLE_NAME).limit(20))  # noqa: F821
