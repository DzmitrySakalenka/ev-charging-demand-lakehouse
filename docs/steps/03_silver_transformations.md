# 03 Silver Transformations

## Goal

Convert raw Bronze tables into typed, standardized, deduplicated, and enriched
Silver tables suitable for analytics.

## Target

- `ev_charging_demo.silver.charging_sessions_clean`
- `ev_charging_demo.silver.weather_hourly_clean`
- `ev_charging_demo.silver.ev_stations_clean`
- `ev_charging_demo.silver.charging_sessions_enriched`

## Ordered commands

1. Run the Silver notebook in Databricks.

```text
notebook/04_build_silver.py
```

2. Validate row preservation.

```sql
SELECT COUNT(*) FROM ev_charging_demo.silver.charging_sessions_clean;
SELECT COUNT(*) FROM ev_charging_demo.silver.weather_hourly_clean;
SELECT COUNT(*) FROM ev_charging_demo.silver.charging_sessions_enriched;
```

3. Validate weather join coverage.

```sql
SELECT
  SUM(CASE WHEN temperature_2m IS NOT NULL THEN 1 ELSE 0 END) AS weather_matched,
  COUNT(*) AS total,
  SUM(CASE WHEN temperature_2m IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) AS match_rate
FROM ev_charging_demo.silver.charging_sessions_enriched;
```

4. Validate station metadata matching.

```sql
SELECT station_match_status, COUNT(*)
FROM ev_charging_demo.silver.charging_sessions_enriched
GROUP BY station_match_status;
```

5. Validate quality flags.

```sql
SELECT
  SUM(CASE WHEN NOT is_valid_start_ts THEN 1 ELSE 0 END) AS invalid_start_ts,
  SUM(CASE WHEN NOT is_valid_energy THEN 1 ELSE 0 END) AS invalid_energy,
  SUM(CASE WHEN NOT is_valid_duration THEN 1 ELSE 0 END) AS invalid_duration,
  SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) AS duplicates,
  SUM(CASE WHEN is_energy_outlier THEN 1 ELSE 0 END) AS energy_outliers,
  SUM(CASE WHEN is_duration_outlier THEN 1 ELSE 0 END) AS duration_outliers
FROM ev_charging_demo.silver.charging_sessions_clean;
```

## Validated results

- Session rows: `20,142`
- Weather rows: `94,320`
- Enriched session rows: `20,142`
- Weather matched rows: `20,141`
- Weather match rate: `99.995%`
- Valid analytical rows: `18,008`
- Station matched by name: `3,946`
- Station unmatched: `16,196`

## References

- Silver notebook: [notebook/04_build_silver.py](../../notebook/04_build_silver.py)
- Original task doc: [docs/03_silver_transformations.md](../03_silver_transformations.md)
