# Reference links

## Databricks architecture and storage

- Medallion lakehouse architecture: https://docs.databricks.com/aws/en/lakehouse/medallion
- What is Delta Lake in Databricks: https://docs.databricks.com/aws/en/delta/
- Unity Catalog: https://docs.databricks.com/aws/en/data-governance/unity-catalog/
- Unity Catalog Volumes: https://docs.databricks.com/aws/en/volumes/

## Databricks ingestion, quality, orchestration, dashboards

- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/
- Lakeflow Jobs: https://docs.databricks.com/aws/en/jobs/
- Create first Lakeflow Job: https://docs.databricks.com/gcp/en/jobs/jobs-quickstart
- Lakeflow expectations / data quality: https://docs.databricks.com/aws/en/ldp/expectations
- Databricks dashboards: https://docs.databricks.com/aws/en/dashboards/

## Data sources

- Town of Cary EV charging stations dataset: https://data.townofcary.org/explore/dataset/electric-vehicle-charging-stations/
- Data.gov mirror for Cary dataset: https://catalog.data.gov/dataset/electric-vehicle-charging-stations
- Open-Meteo Historical Weather API: https://open-meteo.com/en/docs/historical-weather-api
- Alternative Fuel Stations API: https://developer.nlr.gov/docs/transportation/alt-fuel-stations-v1/
- Alternative Fuel Stations API — All Stations: https://developer.nlr.gov/docs/transportation/alt-fuel-stations-v1/all/

## Notes

The Alternative Fuel Stations API documentation currently indicates a domain transition away from `developer.nrel.gov` toward `developer.nlr.gov`. Prefer the domain shown in the current official docs when implementing API calls.
