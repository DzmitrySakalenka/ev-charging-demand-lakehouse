# Databricks notebook source
# ruff: noqa: N999
# 04_build_silver.py
# Purpose: Build Silver tables from Bronze: clean, deduplicate, and enrich
# Cary charging sessions with Open-Meteo weather and (optional) AFDC station
# metadata. Implements docs/03_silver_transformations.md.

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

CATALOG = "ev_charging_demo"
SOURCE_TZ = "America/New_York"

# Sanity bounds for Bronze->Silver quality flags. Any session whose values
# fall outside these ranges is flagged rather than dropped, so analysts can
# decide whether to include them downstream (per docs/03 invariant
# "Do not silently delete records.").
ENERGY_OUTLIER_KWH = 75.0
DURATION_OUTLIER_SECONDS = 28800
MAX_VALID_ENERGY_KWH = 200.0
MAX_VALID_DURATION_SECONDS = 86400
MAX_VALID_AVG_POWER_KW = 400.0
COLD_TEMPERATURE_C = 5
HOT_TEMPERATURE_C = 25
LIGHT_PRECIPITATION_MM = 2

BRONZE_SESSIONS = f"{CATALOG}.bronze.cary_charging_sessions_raw"
BRONZE_WEATHER = f"{CATALOG}.bronze.openmeteo_weather_hourly_raw"
BRONZE_STATIONS = f"{CATALOG}.bronze.afdc_ev_stations_raw"

SILVER_SESSIONS = f"{CATALOG}.silver.charging_sessions_clean"
SILVER_WEATHER = f"{CATALOG}.silver.weather_hourly_clean"
SILVER_STATIONS = f"{CATALOG}.silver.ev_stations_clean"
SILVER_ENRICHED = f"{CATALOG}.silver.charging_sessions_enriched"


def _normalize_text(column: Column) -> Column:
    """Return a lowercase, alphanumeric-only representation of a string column."""
    return F.lower(F.regexp_replace(F.coalesce(column, F.lit("")), r"[^a-zA-Z0-9]+", ""))


def _hh_mm_ss_to_seconds(column: Column) -> Column:
    """Convert an HH:MM:SS duration string into total seconds as an integer."""
    parts = F.split(column.cast("string"), ":")
    return (
        parts.getItem(0).cast("int") * F.lit(3600)
        + parts.getItem(1).cast("int") * F.lit(60)
        + parts.getItem(2).cast("int")
    )


raw_sessions = spark.read.table(BRONZE_SESSIONS)
raw_weather = spark.read.table(BRONZE_WEATHER)

# ============================================================================
# Charging sessions cleaning + quality flags
# ============================================================================

# Cary timestamps carry an explicit offset (e.g. -05:00 / -04:00 across DST),
# so to_timestamp produces a UTC-anchored instant. session_hour_ts is the
# UTC hour bucket that joins against weather below.
sessions_typed = (
    raw_sessions
    .withColumn("start_ts", F.to_timestamp("start_date_time"))
    .withColumn("session_date", F.to_date("start_ts"))
    .withColumn("session_hour", F.hour("start_ts"))
    .withColumn("day_of_week", F.date_format("start_ts", "E"))
    .withColumn("is_weekend", F.dayofweek("start_ts").isin([1, 7]))
    .withColumn("station_name", F.trim(F.col("station_name").cast("string")))
    .withColumn("station_name_normalized", _normalize_text(F.col("station_name")))
    .withColumn("address_normalized", _normalize_text(F.col("address_1")))
    .withColumn(
        "energy_kwh_value",
        F.regexp_replace(F.col("energy_kwh").cast("string"), ",", "").cast("double"),
    )
    .withColumn("duration_seconds", _hh_mm_ss_to_seconds(F.col("charging_time")))
    .withColumn("duration_minutes", F.col("duration_seconds") / F.lit(60.0))
    .withColumn("duration_hours", F.col("duration_seconds") / F.lit(3600.0))
    .withColumn(
        "avg_power_kw",
        F.when(F.col("duration_hours") > 0, F.col("energy_kwh_value") / F.col("duration_hours")),
    )
    .withColumn("session_hour_ts", F.date_trunc("hour", F.col("start_ts")))
    .withColumn(
        "session_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("station_name_normalized"),
                F.col("start_ts").cast("string"),
                F.col("energy_kwh_value").cast("string"),
                F.col("duration_seconds").cast("string"),
            ),
            256,
        ),
    )
    # Bronze keeps `energy_kwh` as a raw STRING; replace it with the typed
    # double under the same name the docs/Silver schema expect.
    .drop("energy_kwh")
    .withColumnRenamed("energy_kwh_value", "energy_kwh")
)

# Flag (do not drop) duplicate sessions. Earliest ingestion wins as canonical.
dedup_window = Window.partitionBy("session_id").orderBy("ingestion_ts")
sessions_clean = (
    sessions_typed
    .withColumn("_dedup_rank", F.row_number().over(dedup_window))
    .withColumn("is_duplicate", F.col("_dedup_rank") > 1)
    .drop("_dedup_rank")
    .withColumn("is_valid_start_ts", F.col("start_ts").isNotNull())
    .withColumn(
        "is_valid_station",
        F.col("station_name").isNotNull() & (F.length(F.col("station_name")) > 0),
    )
    .withColumn(
        "is_valid_energy",
        (F.col("energy_kwh") > 0) & (F.col("energy_kwh") < F.lit(MAX_VALID_ENERGY_KWH)),
    )
    .withColumn(
        "is_valid_duration",
        (F.col("duration_seconds") > 0)
        & (F.col("duration_seconds") <= F.lit(MAX_VALID_DURATION_SECONDS)),
    )
    .withColumn(
        "is_valid_avg_power",
        (F.col("avg_power_kw") > 0) & (F.col("avg_power_kw") < F.lit(MAX_VALID_AVG_POWER_KW)),
    )
    # Static outlier thresholds; revisit with percentile/IQR once we have more
    # months of data and the Silver table can be profiled meaningfully.
    .withColumn("is_energy_outlier", F.col("energy_kwh") > F.lit(ENERGY_OUTLIER_KWH))
    .withColumn(
        "is_duration_outlier",
        F.col("duration_seconds") > F.lit(DURATION_OUTLIER_SECONDS),
    )
)

sessions_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_SESSIONS)

# ============================================================================
# Weather cleaning
# ============================================================================

# Open-Meteo's CSV stores naive local timestamps; the file metadata states
# `timezone=America/New_York`. to_utc_timestamp interprets the naive value as
# Eastern time and emits a UTC instant that aligns with sessions.start_ts.
weather_clean = (
    raw_weather
    .withColumn(
        "weather_hour_ts",
        F.to_utc_timestamp(F.to_timestamp(F.col("time")), SOURCE_TZ),
    )
    .withColumn("temperature_2m", F.col("temperature_2m_c").cast("double"))
    .withColumn("precipitation", F.col("precipitation_mm").cast("double"))
    .withColumn("rain", F.col("rain_mm").cast("double"))
    .withColumn("wind_speed_10m", F.col("wind_speed_10m_km_h").cast("double"))
    .withColumn(
        "temperature_bucket",
        F.when(F.col("temperature_2m") < F.lit(COLD_TEMPERATURE_C), F.lit("cold"))
         .when(F.col("temperature_2m") <= F.lit(HOT_TEMPERATURE_C), F.lit("mild"))
         .otherwise(F.lit("hot")),
    )
    .withColumn(
        "precipitation_bucket",
        F.when(F.col("precipitation") == F.lit(0), F.lit("dry"))
         .when(F.col("precipitation") <= F.lit(LIGHT_PRECIPITATION_MM), F.lit("light"))
         .otherwise(F.lit("wet")),
    )
)

weather_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_WEATHER)

# ============================================================================
# Station metadata cleaning (optional)
# ============================================================================

has_station_metadata = spark.catalog.tableExists(BRONZE_STATIONS)
if has_station_metadata:
    raw_stations = spark.read.table(BRONZE_STATIONS)
    stations_clean = (
        raw_stations
        .select(
            F.col("id").cast("string").alias("station_metadata_id"),
            F.trim(F.col("station_name")).alias("station_name"),
            _normalize_text(F.col("station_name")).alias("station_name_normalized"),
            F.col("street_address").alias("address"),
            _normalize_text(F.col("street_address")).alias("address_normalized"),
            F.col("city"),
            F.col("state"),
            F.col("zip").alias("zip_code"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("ev_network"),
            F.col("access_code"),
            F.col("access_days_time"),
            F.col("ev_level1_evse_num").cast("int").alias("ev_level1_evse_num"),
            F.col("ev_level2_evse_num").cast("int").alias("ev_level2_evse_num"),
            F.col("ev_dc_fast_num").cast("int").alias("ev_dc_fast_num"),
            F.col("ev_connector_types"),
        )
        .dropDuplicates(["station_metadata_id"])
    )
    stations_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_STATIONS)

# ============================================================================
# Enrichment: sessions LEFT weather LEFT stations (optional)
# ============================================================================

enriched = (
    sessions_clean.alias("sess")
    .join(
        weather_clean.alias("wx"),
        F.col("sess.session_hour_ts") == F.col("wx.weather_hour_ts"),
        "left",
    )
)

if has_station_metadata:
    enriched = (
        enriched
        .join(
            stations_clean.alias("st"),
            F.col("sess.station_name_normalized") == F.col("st.station_name_normalized"),
            "left",
        )
        .withColumn(
            "station_match_status",
            F.when(F.col("st.station_metadata_id").isNotNull(), F.lit("matched_by_name"))
             .otherwise(F.lit("unmatched")),
        )
    )
else:
    enriched = enriched.withColumn("station_match_status", F.lit("unmatched"))

# Final projection follows the column list in docs/03_silver_transformations.md
# so the Gold layer can rely on a stable Silver contract.
enriched_final = enriched.select(
    F.col("sess.session_id"),
    F.col("sess.station_name"),
    F.col("sess.station_name_normalized"),
    F.col("sess.start_ts"),
    F.col("sess.session_date"),
    F.col("sess.session_hour"),
    F.col("sess.day_of_week"),
    F.col("sess.is_weekend"),
    F.col("sess.duration_seconds"),
    F.col("sess.duration_minutes"),
    F.col("sess.energy_kwh"),
    F.col("sess.avg_power_kw"),
    F.col("sess.is_valid_start_ts"),
    F.col("sess.is_valid_energy"),
    F.col("sess.is_valid_duration"),
    F.col("sess.is_valid_avg_power"),
    F.col("sess.is_duplicate"),
    F.col("sess.is_energy_outlier"),
    F.col("sess.is_duration_outlier"),
    F.col("wx.temperature_2m"),
    F.col("wx.precipitation"),
    F.col("wx.rain"),
    F.col("wx.wind_speed_10m"),
    F.col("wx.temperature_bucket"),
    F.col("wx.precipitation_bucket"),
    F.col("station_match_status"),
    F.col("sess.source_system"),
    F.col("sess.batch_id"),
    F.col("sess.ingestion_ts"),
)

enriched_final.write.format("delta").mode("overwrite").saveAsTable(SILVER_ENRICHED)

print(f"Created: {SILVER_SESSIONS} (rows: {sessions_clean.count()})")
print(f"Created: {SILVER_WEATHER} (rows: {weather_clean.count()})")
if has_station_metadata:
    print(f"Created: {SILVER_STATIONS} (rows: {stations_clean.count()})")
print(f"Created: {SILVER_ENRICHED} (rows: {enriched_final.count()})")
display(enriched_final.limit(20))  # noqa: F821 (Databricks notebook builtin)
