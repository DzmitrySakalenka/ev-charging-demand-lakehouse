-- Databricks notebook source
-- 07_dashboard_queries.sql
-- Purpose: Presentation-ready queries, one per dashboard tile from
-- docs/06_jobs_and_dashboard.md. Each query stands alone so any single
-- statement can be copied into a Databricks dashboard tile or a slide.
--
-- Reads only from Gold (and one Silver tile for temperature_bucket, which
-- Gold doesn't carry). Keeps business users away from raw and intermediate
-- data, per docs/06 presentation narrative.

-- ============================================================================
-- Tile 1: Top stations by total kWh (bar chart)
-- ============================================================================
SELECT
  station_name,
  SUM(total_energy_kwh) AS total_energy_kwh,
  SUM(sessions_count) AS sessions_count
FROM ev_charging_demo.gold.station_daily_kpis
GROUP BY station_name
ORDER BY total_energy_kwh DESC
LIMIT 10;

-- ============================================================================
-- Tile 2: Sessions and kWh by hour of day (line or bar chart)
-- ============================================================================
SELECT
  session_hour,
  SUM(sessions_count) AS sessions_count,
  SUM(total_energy_kwh) AS total_energy_kwh
FROM ev_charging_demo.gold.station_hourly_demand
GROUP BY session_hour
ORDER BY session_hour;

-- ============================================================================
-- Tile 3: Weekday vs weekend demand (bar chart)
-- ============================================================================
SELECT
  CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
  SUM(sessions_count) AS sessions_count,
  SUM(total_energy_kwh) AS total_energy_kwh
FROM ev_charging_demo.gold.station_daily_kpis
GROUP BY CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END
ORDER BY day_type;

-- ============================================================================
-- Tile 4: Average duration by station (bar chart)
-- ============================================================================
-- Daily averages weighted by the day's session count, so a one-session day
-- with a 12-hour park doesn't dominate a busy station's mean. Reading the
-- two columns together also shows how robust each station's average is.
SELECT
  station_name,
  SUM(sessions_count * avg_duration_minutes) / NULLIF(SUM(sessions_count), 0)
    AS avg_duration_minutes,
  SUM(sessions_count) AS sessions_count
FROM ev_charging_demo.gold.station_daily_kpis
GROUP BY station_name
ORDER BY avg_duration_minutes DESC
LIMIT 15;

-- ============================================================================
-- Tile 5: Energy and duration by temperature bucket (bar chart)
-- ============================================================================
-- Sourced from Silver because `temperature_bucket` is not carried in Gold.
-- Applies the same analytical filter as Gold so headline numbers reconcile.
SELECT
  temperature_bucket,
  COUNT(*) AS sessions_count,
  AVG(energy_kwh) AS avg_energy_kwh,
  AVG(duration_minutes) AS avg_duration_minutes
FROM
  ev_charging_demo.silver.charging_sessions_enriched
WHERE
  is_valid_start_ts = true
  AND is_valid_energy = true
  AND is_valid_duration = true
  AND is_duplicate = false
  AND temperature_bucket IS NOT NULL
GROUP BY
  temperature_bucket
ORDER BY
  sessions_count DESC;

-- ============================================================================
-- Tile 6: Data quality summary (table)
-- ============================================================================
SELECT *
FROM ev_charging_demo.gold.data_quality_summary;
