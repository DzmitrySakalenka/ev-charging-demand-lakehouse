-- Databricks notebook source
-- 06_data_quality_checks.sql
-- Purpose: Validate the data-quality story end-to-end.
--   * Confirms Silver carries every quality flag from docs/05_data_quality.md
--   * Counts records that pass / fail each rule
--   * Exercises the optional `valid_hour` rule
--   * Snapshots Gold.data_quality_summary
--   * Surfaces sample suspicious rows for spot-checking
--
-- Run after notebook/04_build_silver.py and notebook/05_build_gold.sql so all
-- referenced tables exist. Each section emits one result set in the notebook
-- output, intentionally — they double as the on-screen quality dashboard.

-- ============================================================================
-- 1. Confirm Silver flag columns exist (schema introspection)
-- ============================================================================
DESCRIBE TABLE ev_charging_demo.silver.charging_sessions_clean;

-- ============================================================================
-- 2. Per-rule pass / fail counts (one row per rule from docs/05_data_quality.md)
-- ============================================================================
-- `records_ok` is the count we want to be high; `records_flagged` is the
-- count we expect to be low and is the one we eyeball for outliers.
-- For duplicate / outlier rules the labels are inverted so the same column
-- meaning holds across the table (flagged = "needs human attention").
SELECT 'valid_start_ts' AS rule_name,
       SUM(CASE WHEN is_valid_start_ts THEN 1 ELSE 0 END) AS records_ok,
       SUM(CASE WHEN NOT is_valid_start_ts THEN 1 ELSE 0 END) AS records_flagged
FROM ev_charging_demo.silver.charging_sessions_clean
UNION ALL
SELECT 'valid_station',
       SUM(CASE WHEN is_valid_station THEN 1 ELSE 0 END),
       SUM(CASE WHEN NOT is_valid_station THEN 1 ELSE 0 END)
FROM ev_charging_demo.silver.charging_sessions_clean
UNION ALL
SELECT 'valid_energy',
       SUM(CASE WHEN is_valid_energy THEN 1 ELSE 0 END),
       SUM(CASE WHEN NOT is_valid_energy THEN 1 ELSE 0 END)
FROM ev_charging_demo.silver.charging_sessions_clean
UNION ALL
SELECT 'valid_duration',
       SUM(CASE WHEN is_valid_duration THEN 1 ELSE 0 END),
       SUM(CASE WHEN NOT is_valid_duration THEN 1 ELSE 0 END)
FROM ev_charging_demo.silver.charging_sessions_clean
UNION ALL
SELECT 'valid_avg_power',
       COUNT_IF(is_valid_avg_power IS TRUE),
       COUNT_IF(is_valid_avg_power IS NOT TRUE)
FROM ev_charging_demo.silver.charging_sessions_clean
UNION ALL
SELECT 'not_duplicate',
       SUM(CASE WHEN NOT is_duplicate THEN 1 ELSE 0 END),
       SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END)
FROM ev_charging_demo.silver.charging_sessions_clean
UNION ALL
SELECT 'energy_outlier',
       SUM(CASE WHEN NOT is_energy_outlier THEN 1 ELSE 0 END),
       SUM(CASE WHEN is_energy_outlier THEN 1 ELSE 0 END)
FROM ev_charging_demo.silver.charging_sessions_clean
UNION ALL
SELECT 'duration_outlier',
       SUM(CASE WHEN NOT is_duration_outlier THEN 1 ELSE 0 END),
       SUM(CASE WHEN is_duration_outlier THEN 1 ELSE 0 END)
FROM ev_charging_demo.silver.charging_sessions_clean;

-- ============================================================================
-- 3. Optional `valid_hour` rule from docs/05_data_quality.md
-- ============================================================================
-- `session_hour` is derived from `start_ts`, so an out-of-range value can
-- only happen if start_ts was malformed. This is a defensive cross-check
-- rather than a standalone flag in Silver.
SELECT
    SUM(CASE WHEN session_hour BETWEEN 0 AND 23 THEN 1 ELSE 0 END) AS records_ok,
    SUM(CASE
            WHEN session_hour IS NULL OR session_hour NOT BETWEEN 0 AND 23
            THEN 1 ELSE 0
        END) AS records_flagged
FROM ev_charging_demo.silver.charging_sessions_clean;

-- ============================================================================
-- 4. Gold data quality summary -- the trust snapshot consumers should read
-- ============================================================================
SELECT * FROM ev_charging_demo.gold.data_quality_summary;

-- ============================================================================
-- 5. Flag-combination matrix: where invalid energy and invalid duration overlap
-- ============================================================================
-- Answers: "Are the same rows flagged on both rules, or are they independent
-- failures?" A diagonal-heavy result means the same suspect sessions trip
-- both checks; a spread-out result means we have two distinct populations.
SELECT
    is_valid_energy,
    is_valid_duration,
    COUNT(*) AS rows
FROM ev_charging_demo.silver.charging_sessions_clean
GROUP BY is_valid_energy, is_valid_duration
ORDER BY rows DESC;

-- ============================================================================
-- 6. Sample suspicious rows -- for the "explain why some records are flagged
--    but not deleted" definition-of-done item in docs/05
-- ============================================================================
SELECT
    session_id,
    station_name,
    start_ts,
    energy_kwh,
    duration_seconds,
    avg_power_kw,
    is_valid_start_ts,
    is_valid_energy,
    is_valid_duration,
    is_valid_avg_power,
    is_energy_outlier,
    is_duration_outlier
FROM ev_charging_demo.silver.charging_sessions_clean
WHERE NOT is_valid_start_ts
   OR NOT is_valid_energy
   OR NOT is_valid_duration
   OR is_energy_outlier
   OR is_duration_outlier
ORDER BY start_ts
LIMIT 20;
