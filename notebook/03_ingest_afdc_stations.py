# Databricks notebook source
# Copyright (c) 2026 Dzmitry Sakalenka.
# ruff: noqa: N999
# Purpose: Fetch NLR AFDC public EV stations via API and land them in Bronze.
# Note: As of May 2026 the AFDC developer portal migrated from
# developer.nrel.gov to developer.nlr.gov; the legacy domain returns 410 Gone
# during the transition window and is shut down on 2026-05-29.

import json
import re
from datetime import UTC, datetime

import requests
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "ev_charging_demo"
BRONZE_SCHEMA = "bronze"
TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.afdc_ev_stations_raw"

# AFDC "all stations" endpoint, filtered server-side to public, operational
# electric stations in Cary, NC. The API key lives in a Databricks secret
# scope so it never reaches source control, env vars, or notebook output.
API_URL = "https://developer.nlr.gov/api/alt-fuel-stations/v1.json"
API_PARAMS = {
    "fuel_type": "ELEC",
    "state": "NC",
    "city": "Cary",
    "status": "E",
    "access": "public",
    "limit": "all",
}
SOURCE_URL = "https://developer.nlr.gov/docs/transportation/alt-fuel-stations-v1/all/"

# Each ingestion run is snapshotted under the AFDC volume folder so Bronze
# can be rebuilt without re-hitting the API. This satisfies the "Store raw
# JSON/CSV response" step in docs/02_bronze_layer.md while still treating
# the API as the source of truth.
RAW_SNAPSHOT_DIR = "/Volumes/ev_charging_demo/bronze/raw_files/afdc_stations"
BATCH_ID = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
SNAPSHOT_PATH = f"{RAW_SNAPSHOT_DIR}/AFDC_{BATCH_ID}.json"

api_key = dbutils.secrets.get(scope="ev-charging-demand", key="afdc_api_key")

response = requests.get(
    API_URL,
    params={**API_PARAMS, "api_key": api_key},
    timeout=60,
)
response.raise_for_status()
payload = response.json()

dbutils.fs.put(SNAPSHOT_PATH, json.dumps(payload), overwrite=True)
print(f"Wrote raw snapshot: {SNAPSHOT_PATH}")
print(f"Total stations returned: {payload.get('total_results')}")

# Re-read the snapshot through Spark so nested fields (ev_connector_types,
# related_stations, ...) survive as ARRAY/STRUCT instead of being
# stringified. `multiLine` is required because the AFDC payload is a single
# JSON object that spans the whole file.
payload_df = spark.read.option("multiLine", True).json(SNAPSHOT_PATH)

stations_df = payload_df.select(F.explode("fuel_stations").alias("station")).select("station.*")


def _sanitize(name: str) -> str:
    """Normalize a source column name to a Delta-safe snake_case identifier."""
    return re.sub(r"_+", "_", re.sub(r"[^0-9a-z]+", "_", name.lower())).strip("_")


# AFDC fields are already snake_case, so the rename is typically a no-op.
# We still run it so the notebook stays robust against future schema
# additions that may include casing or punctuation Delta rejects without
# column mapping.
renamed_df = stations_df.toDF(*(_sanitize(c) for c in stations_df.columns))
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
    .withColumn("source_system", F.lit("afdc_nlr"))
    .withColumn("source_url", F.lit(SOURCE_URL))
    .withColumn("source_file", F.lit(SNAPSHOT_PATH))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("batch_id", F.lit(BATCH_ID))
)

bronze_df.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)

print(f"Created table: {TABLE_NAME}")
print(f"Rows: {bronze_df.count()}")
bronze_df.printSchema()
display(bronze_df.limit(20))  # noqa: F821 (Databricks notebook builtin)
