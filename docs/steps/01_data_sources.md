# 01 Data Sources

## Goal

Define the raw data sources, their scope, their grain, and the files used by the
Bronze ingestion notebooks.

## Target

- Cary charging sessions source selected and downloaded
- Open-Meteo weather source selected and downloaded for the matching date range
- AFDC/NLR station metadata scope defined as North Carolina public EV stations
- Raw files present locally under `raw/`

## Ordered commands

1. Confirm local raw files exist.

```bash
find raw -maxdepth 1 -type f -printf '%p %s bytes\n'
```

2. Confirm expected files are present.

```text
raw/electric-vehicle-charging-stations.csv
raw/open-meteo.csv
raw/AFDC.json
```

## Data source scope

- Cary sessions: Town of Cary public EV charging session records
- Weather: Open-Meteo hourly historical weather for Cary, NC
- AFDC/NLR: North Carolina public EV station metadata

The AFDC/NLR source is intentionally broader than Cary because the All Stations
endpoint appears to ignore the `city=Cary` parameter. Cary matching is handled
later as a Silver enrichment concern.

## References

- Local raw files: [raw](../../raw)
- Source contract doc: [docs/01_data_sources.md](../01_data_sources.md)
