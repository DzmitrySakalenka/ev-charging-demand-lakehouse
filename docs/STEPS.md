# Implementation Steps

This document is the main runbook for the EV Charging Demand Lakehouse demo.
Each step links to its own file with the goal, target outputs, ordered
commands, validation queries, and implementation references.

## Pipeline order

```text
00_workspace_setup
01_data_sources
02_bronze_layer
03_silver_transformations
04_gold_layer
05_data_quality
06_jobs_and_dashboard
```

The deployed Databricks job runs the executable pipeline as:

```text
ingest_cary_sessions
ingest_openmeteo_weather
ingest_afdc_stations
  -> build_silver
  -> build_gold
  -> data_quality_checks
```

## Step files

- [00 Workspace Setup](steps/00_workspace_setup.md)
- [01 Data Sources](steps/01_data_sources.md)
- [02 Bronze Layer](steps/02_bronze_layer.md)
- [03 Silver Transformations](steps/03_silver_transformations.md)
- [04 Gold Layer](steps/04_gold_layer.md)
- [05 Data Quality](steps/05_data_quality.md)
- [06 Jobs And Dashboard](steps/06_jobs_and_dashboard.md)

## Important project files

- Bundle config: [databricks.yml](../databricks.yml)
- Pipeline job: [resources/ev_charging_pipeline.job.yml](../resources/ev_charging_pipeline.job.yml)
- Dashboard resource: [resources/ev_charging_overview.dashboard.yml](../resources/ev_charging_overview.dashboard.yml)
- Dashboard JSON: [resources/dashboards/ev_charging_overview.lvdash.json](../resources/dashboards/ev_charging_overview.lvdash.json)

## Validation snapshot

- Bundle validation: `databricks bundle validate --profile DEFAULT -t dev`
- Pipeline job run: success
- Dashboard: active
- Gold tables reconcile to `18,008` valid analytical session rows
- SQL warehouse is stopped after validation runs
