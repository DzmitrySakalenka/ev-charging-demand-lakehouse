# 06 Jobs And Dashboard

## Goal

Show that the notebooks are a reproducible Databricks pipeline with a
presentation-ready dashboard, not isolated scripts.

## Target

- Bundle job: `ev_charging_pipeline`
- Dashboard: `EV Charging Overview`
- Dashboard query notebook: `notebook/07_dashboard_queries.sql`
- Dashboard resource: `resources/ev_charging_overview.dashboard.yml`

## Ordered commands

1. Validate the Databricks bundle.

```bash
databricks bundle validate --profile DEFAULT -t dev
```

2. Deploy the bundle resources.

```bash
databricks bundle deploy --profile DEFAULT -t dev
```

3. Run the full pipeline job.

```bash
databricks bundle run ev_charging_pipeline --profile DEFAULT -t dev
```

4. Verify the deployed job exists.

```bash
databricks jobs list --profile DEFAULT
```

5. Verify the dashboard exists.

```bash
databricks lakeview list --profile DEFAULT
```

6. Run dashboard queries.

```text
notebook/07_dashboard_queries.sql
```

## Validated results

- Bundle validation passed
- Job `[dev dzmitrysakalenka] ev_charging_pipeline` exists
- One full job run completed with `SUCCESS`
- Dashboard `[dev dzmitrysakalenka] EV Charging Overview` is active
- Dashboard query result shapes:
  - Top stations: `10` rows
  - Demand by hour: `24` rows
  - Weekday vs weekend: `2` rows
  - Average duration by station: `15` rows
  - Temperature buckets: `3` rows
  - Data quality summary: `1` row

## References

- Pipeline resource: [resources/ev_charging_pipeline.job.yml](../../resources/ev_charging_pipeline.job.yml)
- Dashboard resource: [resources/ev_charging_overview.dashboard.yml](../../resources/ev_charging_overview.dashboard.yml)
- Dashboard JSON: [resources/dashboards/ev_charging_overview.lvdash.json](../../resources/dashboards/ev_charging_overview.lvdash.json)
- Dashboard query notebook: [notebook/07_dashboard_queries.sql](../../notebook/07_dashboard_queries.sql)
- Original task doc: [docs/06_jobs_and_dashboard.md](../06_jobs_and_dashboard.md)
