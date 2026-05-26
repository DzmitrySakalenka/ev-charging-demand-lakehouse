-- Databricks notebook source
-- 05_build_gold.sql
-- Purpose: Business-facing KPI tables built from the validated Silver subset.
--
-- Analytical filter applied to all KPI tables:
--   is_valid_start_ts = true
--   AND is_valid_energy = true
--   AND is_valid_duration = true
--   AND is_duplicate = false
--
-- (Mirrors docs/03 "clean analytical subset" guidance and docs/04 build rules.)

-- ============================================================================
-- 1. station_daily_kpis -- per-station usage by day
-- ============================================================================
-- peak_hour is the hour-of-day with the largest TOTAL energy at that station
-- on that date, so a single oversized session can't outweigh a busier hour.
CREATE OR REPLACE TABLE ev_charging_demo.gold.station_daily_kpis
COMMENT 'Per-station daily usage KPIs from the valid Silver subset.'
AS
WITH valid_sessions AS (
  SELECT *
  FROM ev_charging_demo.silver.charging_sessions_enriched
  WHERE is_valid_start_ts = true
    AND is_valid_energy = true
    AND is_valid_duration = true
    AND is_duplicate = false
),
hourly_totals AS (
  SELECT
    station_name,
    session_date,
    session_hour,
    SUM(energy_kwh) AS hour_kwh
  FROM valid_sessions
  GROUP BY station_name, session_date, session_hour
),
peak_hour_per_day AS (
  SELECT
    station_name,
    session_date,
    MAX_BY(session_hour, hour_kwh) AS peak_hour
  FROM hourly_totals
  GROUP BY station_name, session_date
),
daily_aggregates AS (
  SELECT
    station_name,
    session_date,
    COUNT(*) AS sessions_count,
    SUM(energy_kwh) AS total_energy_kwh,
    AVG(energy_kwh) AS avg_energy_kwh,
    percentile_approx(energy_kwh, 0.5) AS median_energy_kwh,
    AVG(duration_minutes) AS avg_duration_minutes,
    percentile_approx(duration_minutes, 0.5) AS median_duration_minutes,
    AVG(avg_power_kw) AS avg_power_kw,
    AVG(temperature_2m) AS avg_temperature_2m,
    SUM(precipitation) AS total_precipitation,
    MAX(CAST(is_weekend AS INT)) = 1 AS is_weekend
  FROM valid_sessions
  GROUP BY station_name, session_date
)
SELECT
  d.station_name,
  d.session_date,
  d.sessions_count,
  d.total_energy_kwh,
  d.avg_energy_kwh,
  d.median_energy_kwh,
  d.avg_duration_minutes,
  d.median_duration_minutes,
  d.avg_power_kw,
  d.avg_temperature_2m,
  d.total_precipitation,
  p.peak_hour,
  d.is_weekend
FROM daily_aggregates d
LEFT JOIN peak_hour_per_day p USING (station_name, session_date);

-- ============================================================================
-- 2. station_hourly_demand -- demand shape by station, weekday, and hour
-- ============================================================================
CREATE OR REPLACE TABLE ev_charging_demo.gold.station_hourly_demand
COMMENT 'Hourly demand shape per station and weekday.'
AS
SELECT
  station_name,
  day_of_week,
  session_hour,
  COUNT(*) AS sessions_count,
  SUM(energy_kwh) AS total_energy_kwh,
  AVG(duration_minutes) AS avg_duration_minutes,
  AVG(temperature_2m) AS avg_temperature_2m
FROM ev_charging_demo.silver.charging_sessions_enriched
WHERE is_valid_start_ts = true
  AND is_valid_energy = true
  AND is_valid_duration = true
  AND is_duplicate = false
GROUP BY station_name, day_of_week, session_hour;

-- ============================================================================
-- 3. station_utilization_proxy -- relative demand ranking, NOT real utilization
-- ============================================================================
-- This is a PROXY only: derived from session-level activity, not from port
-- occupancy or charger telemetry. Use the ranking as a relative-demand signal,
-- not as a true utilization rate.
--
-- peak_hour_share = (sessions in the busiest hour-of-day for that station)
--                 / (total sessions for that station)
-- Higher values mean demand concentrates in a narrow window; lower values
-- mean demand is spread across the day.
CREATE OR REPLACE TABLE ev_charging_demo.gold.station_utilization_proxy
COMMENT 'Proxy ranking of station demand from session activity. Not a true port-level utilization metric.'
AS
WITH valid_sessions AS (
  SELECT *
  FROM ev_charging_demo.silver.charging_sessions_enriched
  WHERE is_valid_start_ts = true
    AND is_valid_energy = true
    AND is_valid_duration = true
    AND is_duplicate = false
),
station_hour_sessions AS (
  SELECT
    station_name,
    session_hour,
    COUNT(*) AS hour_sessions
  FROM valid_sessions
  GROUP BY station_name, session_hour
),
peak_hour_per_station AS (
  SELECT
    station_name,
    MAX(hour_sessions) AS peak_hour_sessions
  FROM station_hour_sessions
  GROUP BY station_name
),
station_base AS (
  SELECT
    station_name,
    COUNT(DISTINCT session_date) AS active_days,
    COUNT(*) AS sessions_total,
    SUM(energy_kwh) AS total_energy_kwh
  FROM valid_sessions
  GROUP BY station_name
)
SELECT
  b.station_name,
  b.active_days,
  b.sessions_total,
  b.total_energy_kwh,
  b.sessions_total / NULLIF(b.active_days, 0) AS avg_sessions_per_day,
  b.total_energy_kwh / NULLIF(b.active_days, 0) AS avg_kwh_per_day,
  p.peak_hour_sessions / NULLIF(b.sessions_total, 0) AS peak_hour_share,
  b.sessions_total + b.total_energy_kwh / 10 AS demand_score
FROM station_base b
LEFT JOIN peak_hour_per_station p USING (station_name);

-- ============================================================================
-- 4. data_quality_summary -- pipeline-level trust snapshot
-- ============================================================================
-- Single-row snapshot keyed on run_date. Source counts come straight from
-- Bronze and Silver so this table answers "Can we trust the pipeline?"
-- without re-scanning the underlying tables in the dashboard layer.
CREATE OR REPLACE TABLE ev_charging_demo.gold.data_quality_summary
COMMENT 'Pipeline data-quality snapshot for the latest run.'
AS
WITH bronze_count AS (
  SELECT COUNT(*) AS records_bronze
  FROM ev_charging_demo.bronze.cary_charging_sessions_raw
),
silver_stats AS (
  SELECT
    COUNT(*) AS records_silver,
    SUM(CASE
          WHEN is_valid_start_ts AND is_valid_energy AND is_valid_duration AND NOT is_duplicate
          THEN 1 ELSE 0
        END) AS records_valid,
    SUM(CASE WHEN NOT is_valid_energy THEN 1 ELSE 0 END) AS records_invalid_energy,
    SUM(CASE WHEN NOT is_valid_duration THEN 1 ELSE 0 END) AS records_invalid_duration,
    SUM(CASE WHEN NOT is_valid_station THEN 1 ELSE 0 END) AS records_missing_station,
    SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) AS duplicate_records
  FROM ev_charging_demo.silver.charging_sessions_clean
)
SELECT
  current_date() AS run_date,
  'cary_open_data' AS source_system,
  bronze_count.records_bronze,
  silver_stats.records_silver,
  silver_stats.records_valid,
  silver_stats.records_invalid_energy,
  silver_stats.records_invalid_duration,
  silver_stats.records_missing_station,
  silver_stats.duplicate_records,
  silver_stats.records_valid * 1.0 / NULLIF(silver_stats.records_silver, 0) AS valid_record_rate
FROM bronze_count CROSS JOIN silver_stats;

-- ============================================================================
-- Validation: row counts for all four Gold tables
-- ============================================================================
SELECT 'station_daily_kpis' AS table_name, COUNT(*) AS row_count FROM ev_charging_demo.gold.station_daily_kpis
UNION ALL
SELECT 'station_hourly_demand', COUNT(*) FROM ev_charging_demo.gold.station_hourly_demand
UNION ALL
SELECT 'station_utilization_proxy', COUNT(*) FROM ev_charging_demo.gold.station_utilization_proxy
UNION ALL
SELECT 'data_quality_summary', COUNT(*) FROM ev_charging_demo.gold.data_quality_summary;

-- ============================================================================
-- Dashboard-ready checks (from docs/04_gold_layer.md)
-- ============================================================================

-- Top stations by total kWh
SELECT
  station_name,
  SUM(total_energy_kwh) AS total_energy_kwh,
  SUM(sessions_count) AS sessions_count
FROM ev_charging_demo.gold.station_daily_kpis
GROUP BY station_name
ORDER BY total_energy_kwh DESC
LIMIT 10;

-- Hourly demand
SELECT
  session_hour,
  SUM(sessions_count) AS sessions_count,
  SUM(total_energy_kwh) AS total_energy_kwh
FROM ev_charging_demo.gold.station_hourly_demand
GROUP BY session_hour
ORDER BY session_hour;

-- Weather impact
SELECT
  temperature_bucket,
  COUNT(*) AS sessions_count,
  AVG(energy_kwh) AS avg_energy_kwh,
  AVG(duration_minutes) AS avg_duration_minutes
FROM ev_charging_demo.silver.charging_sessions_enriched
WHERE is_valid_start_ts = true
  AND is_valid_energy = true
  AND is_valid_duration = true
  AND is_duplicate = false
GROUP BY temperature_bucket
ORDER BY sessions_count DESC;
