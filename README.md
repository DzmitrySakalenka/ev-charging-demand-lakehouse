# EV Charging Demand Lakehouse

A compact Databricks lakehouse demo for EV charging demand analytics.

The project ingests public EV charging session data, enriches it with historical
weather and station metadata, validates data quality, and publishes Gold tables
and dashboard-ready queries for operational analysis.

Main business question:

```text
Where and when does EV charging demand peak, and what station-level KPIs can
support operational decisions?
```

## Architecture

```text
Raw files and APIs
  -> Bronze Delta tables
  -> Silver cleaned and enriched tables
  -> Gold KPI and data quality tables
  -> Databricks Job and AI/BI dashboard
```

The implemented pipeline uses:

- Unity Catalog catalog: `ev_charging_demo`
- Schemas: `bronze`, `silver`, `gold`
- Raw file volume: `ev_charging_demo.bronze.raw_files`
- Databricks Asset Bundle job: `ev_charging_pipeline`
- Dashboard: `EV Charging Overview`

## Data sources

- Town of Cary EV charging sessions
  - Source: `https://data.townofcary.org/explore/dataset/electric-vehicle-charging-stations/`
  - Grain: one charging session
  - Local file: `raw/electric-vehicle-charging-stations.csv`
  - Bronze table: `ev_charging_demo.bronze.cary_charging_sessions_raw`

- Open-Meteo historical weather
  - Source: `https://open-meteo.com/en/docs/historical-weather-api`
  - Grain: one hourly weather record for Cary, NC
  - Local file: `raw/open-meteo.csv`
  - Bronze table: `ev_charging_demo.bronze.openmeteo_weather_hourly_raw`

- AFDC/NLR alternative fuel stations
  - Source: `https://developer.nlr.gov/docs/transportation/alt-fuel-stations-v1/`
  - Grain: one North Carolina public EV station metadata record
  - Local file: `raw/AFDC.json`
  - Bronze table: `ev_charging_demo.bronze.afdc_ev_stations_raw`

## Repository organization

```text
.
  README.md
  databricks.yml
  resources/
    ev_charging_pipeline.job.yml
    ev_charging_overview.dashboard.yml
    dashboards/
      ev_charging_overview.lvdash.json
  raw/
    electric-vehicle-charging-stations.csv
    open-meteo.csv
    AFDC.json
  notebook/
    00_smoke_test.py
    01_ingest_cary_sessions.py
    02_ingest_weather_openmeteo.py
    03_ingest_afdc_stations.py
    04_build_silver.py
    05_build_gold.sql
    06_data_quality_checks.sql
    07_dashboard_queries.sql
  docs/
    STEPS.md
    steps/
      00_workspace_setup.md
      01_data_sources.md
      02_bronze_layer.md
      03_silver_transformations.md
      04_gold_layer.md
      05_data_quality.md
      06_jobs_and_dashboard.md
```

## Implemented tables

Bronze:

- `ev_charging_demo.bronze.cary_charging_sessions_raw`
- `ev_charging_demo.bronze.openmeteo_weather_hourly_raw`
- `ev_charging_demo.bronze.afdc_ev_stations_raw`

Silver:

- `ev_charging_demo.silver.charging_sessions_clean`
- `ev_charging_demo.silver.weather_hourly_clean`
- `ev_charging_demo.silver.ev_stations_clean`
- `ev_charging_demo.silver.charging_sessions_enriched`

Gold:

- `ev_charging_demo.gold.station_daily_kpis`
- `ev_charging_demo.gold.station_hourly_demand`
- `ev_charging_demo.gold.station_utilization_proxy`
- `ev_charging_demo.gold.data_quality_summary`

## How to run

Validate and deploy the Databricks bundle:

```bash
databricks bundle validate --profile DEFAULT -t dev
databricks bundle deploy --profile DEFAULT -t dev
```

Run the full pipeline:

```bash
databricks bundle run ev_charging_pipeline --profile DEFAULT -t dev
```

The job runs Bronze ingestion, Silver transformation, Gold KPI creation, and
data-quality checks in dependency order.

For detailed commands, see [docs/STEPS.md](docs/STEPS.md).

## Current status

The demo pipeline has been run successfully end to end.

Key validated numbers:

- Bronze Cary sessions: `20,142`
- Bronze Open-Meteo hourly weather rows: `94,320`
- Bronze AFDC station records: `1,915`
- Valid analytical session rows used by Gold: `18,008`
- Gold daily KPI rows: `6,816`
- Gold hourly demand rows: `1,936`
- Gold utilization proxy rows: `22`
- Data quality valid record rate: `89.4%`
