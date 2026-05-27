# 00 Workspace Setup

## Goal

Prepare a Databricks workspace that can run notebooks, store Delta tables, use a
Unity Catalog volume for raw files, and deploy resources through a Databricks
Asset Bundle.

## Target

- Catalog: `ev_charging_demo`
- Schemas: `bronze`, `silver`, `gold`
- Volume: `ev_charging_demo.bronze.raw_files`
- Bundle target: `dev`
- Smoke table: `ev_charging_demo.bronze.smoke_test`

## Ordered commands

1. Create catalog, schemas, and raw volume in Databricks SQL.

```sql
CREATE CATALOG IF NOT EXISTS ev_charging_demo
MANAGED LOCATION 's3://databricks-storage-7474652295789848/unity-catalog/7474652295789848';

CREATE SCHEMA IF NOT EXISTS ev_charging_demo.bronze;
CREATE SCHEMA IF NOT EXISTS ev_charging_demo.silver;
CREATE SCHEMA IF NOT EXISTS ev_charging_demo.gold;

CREATE VOLUME IF NOT EXISTS ev_charging_demo.bronze.raw_files;
```

2. Run the smoke-test notebook.

```text
notebook/00_smoke_test.py
```

3. Validate the smoke-test table.

```sql
SELECT * FROM ev_charging_demo.bronze.smoke_test;
```

4. Validate the Databricks bundle.

```bash
databricks bundle validate --profile DEFAULT -t dev
```

## References

- Notebook: [notebook/00_smoke_test.py](../../notebook/00_smoke_test.py)
- Bundle config: [databricks.yml](../../databricks.yml)
- Original task doc: [docs/00_first_time_databricks_setup.md](../00_first_time_databricks_setup.md)
