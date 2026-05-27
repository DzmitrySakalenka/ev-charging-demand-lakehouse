# 02 Bronze Layer

## Goal

Land raw data into Delta tables with minimal transformation, preserving source
records and adding ingestion metadata for auditability.

## Target

Core Bronze tables:

- `ev_charging_demo.bronze.cary_charging_sessions_raw`
- `ev_charging_demo.bronze.openmeteo_weather_hourly_raw`
- `ev_charging_demo.bronze.afdc_ev_stations_raw`

Auto Loader comparison tables:

- `ev_charging_demo.bronze.cary_charging_sessions_raw_autoloader`
- `ev_charging_demo.bronze.openmeteo_weather_hourly_raw_autoloader`
- `ev_charging_demo.bronze.afdc_ev_stations_raw_autoloader`

## Ordered commands

1. Create raw landing folders in the Unity Catalog volume.

```bash
databricks fs mkdir dbfs:/Volumes/ev_charging_demo/bronze/raw_files/cary_sessions --profile DEFAULT
databricks fs mkdir dbfs:/Volumes/ev_charging_demo/bronze/raw_files/openmeteo_weather --profile DEFAULT
databricks fs mkdir dbfs:/Volumes/ev_charging_demo/bronze/raw_files/afdc_stations --profile DEFAULT
```

2. Upload raw files to the volume.

```bash
databricks fs cp raw/electric-vehicle-charging-stations.csv dbfs:/Volumes/ev_charging_demo/bronze/raw_files/cary_sessions/electric-vehicle-charging-stations.csv --profile DEFAULT --overwrite
databricks fs cp raw/open-meteo.csv dbfs:/Volumes/ev_charging_demo/bronze/raw_files/openmeteo_weather/meteo.csv --profile DEFAULT --overwrite
databricks fs cp raw/AFDC.json dbfs:/Volumes/ev_charging_demo/bronze/raw_files/afdc_stations/AFDC.json --profile DEFAULT --overwrite
```

3. Verify the raw files in the volume.

```bash
databricks fs ls dbfs:/Volumes/ev_charging_demo/bronze/raw_files/cary_sessions --profile DEFAULT
databricks fs ls dbfs:/Volumes/ev_charging_demo/bronze/raw_files/openmeteo_weather --profile DEFAULT
databricks fs ls dbfs:/Volumes/ev_charging_demo/bronze/raw_files/afdc_stations --profile DEFAULT
```

4. Run the Bronze notebooks in Databricks.

```text
notebook/01_ingest_cary_sessions.py
notebook/02_ingest_weather_openmeteo.py
notebook/03_ingest_afdc_stations.py
```

5. Optionally run the Auto Loader comparison notebooks.

```text
notebook/01_ingest_cary_sessions_autoloader.py
notebook/02_ingest_weather_openmeteo_autoloader.py
notebook/03_ingest_afdc_stations_autoloader.py
```

6. Validate Bronze row counts.

```sql
SELECT COUNT(*) FROM ev_charging_demo.bronze.cary_charging_sessions_raw;
SELECT COUNT(*) FROM ev_charging_demo.bronze.openmeteo_weather_hourly_raw;
SELECT COUNT(*) FROM ev_charging_demo.bronze.afdc_ev_stations_raw;
```

## Validated counts

- Cary sessions: `20,142`
- Open-Meteo weather: `94,320`
- AFDC/NLR station metadata: `1,915`

## References

- Bronze notebooks: [notebook](../../notebook)
- Raw files: [raw](../../raw)
- Original task doc: [docs/02_bronze_layer.md](../02_bronze_layer.md)
