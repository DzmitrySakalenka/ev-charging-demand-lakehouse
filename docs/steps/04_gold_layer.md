# 04 Gold Layer

## Goal

Create business-facing KPI tables that answer operational questions without
requiring raw or row-level Silver exploration.

## Target

- `ev_charging_demo.gold.station_daily_kpis`
- `ev_charging_demo.gold.station_hourly_demand`
- `ev_charging_demo.gold.station_utilization_proxy`
- `ev_charging_demo.gold.data_quality_summary`

## Ordered commands

1. Run the Gold SQL notebook in Databricks.

```text
notebook/05_build_gold.sql
```

2. Validate Gold table row counts.

```sql
SELECT COUNT(*) FROM ev_charging_demo.gold.station_daily_kpis;
SELECT COUNT(*) FROM ev_charging_demo.gold.station_hourly_demand;
SELECT COUNT(*) FROM ev_charging_demo.gold.station_utilization_proxy;
SELECT COUNT(*) FROM ev_charging_demo.gold.data_quality_summary;
```

3. Validate that Gold reconciles to the valid Silver subset.

```sql
WITH valid_sessions AS (
  SELECT COUNT(*) AS valid_rows
  FROM ev_charging_demo.silver.charging_sessions_enriched
  WHERE is_valid_start_ts = true
    AND is_valid_energy = true
    AND is_valid_duration = true
    AND is_duplicate = false
),
gold_totals AS (
  SELECT
    (SELECT SUM(sessions_count) FROM ev_charging_demo.gold.station_daily_kpis) AS daily_sessions,
    (SELECT SUM(sessions_count) FROM ev_charging_demo.gold.station_hourly_demand) AS hourly_sessions,
    (SELECT SUM(sessions_total) FROM ev_charging_demo.gold.station_utilization_proxy) AS utilization_sessions
)
SELECT *
FROM valid_sessions CROSS JOIN gold_totals;
```

4. Run dashboard-style checks.

```sql
SELECT station_name, SUM(total_energy_kwh) AS total_energy_kwh
FROM ev_charging_demo.gold.station_daily_kpis
GROUP BY station_name
ORDER BY total_energy_kwh DESC
LIMIT 10;
```

## Validated results

- `station_daily_kpis`: `6,816` rows
- `station_hourly_demand`: `1,936` rows
- `station_utilization_proxy`: `22` rows
- `data_quality_summary`: `1` row
- All three KPI tables reconcile to `18,008` valid Silver sessions

## References

- Gold notebook: [notebook/05_build_gold.sql](../../notebook/05_build_gold.sql)
- Dashboard queries: [notebook/07_dashboard_queries.sql](../../notebook/07_dashboard_queries.sql)
- Original task doc: [docs/04_gold_layer.md](../04_gold_layer.md)
