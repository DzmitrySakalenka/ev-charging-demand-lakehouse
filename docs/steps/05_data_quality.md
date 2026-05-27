# 05 Data Quality

## Goal

Show that the pipeline validates data and makes questionable records visible
instead of silently dropping them.

## Target

- Quality flags in `ev_charging_demo.silver.charging_sessions_clean`
- Quality summary in `ev_charging_demo.gold.data_quality_summary`
- Validation proof notebook: `notebook/06_data_quality_checks.sql`

## Ordered commands

1. Run the Silver and Gold notebooks first.

```text
notebook/04_build_silver.py
notebook/05_build_gold.sql
```

2. Run the data-quality checks notebook.

```text
notebook/06_data_quality_checks.sql
```

3. Confirm Silver quality columns exist.

```sql
DESCRIBE TABLE ev_charging_demo.silver.charging_sessions_clean;
```

4. Confirm the Gold quality summary.

```sql
SELECT *
FROM ev_charging_demo.gold.data_quality_summary;
```

5. Confirm the invalid energy and duration overlap.

```sql
SELECT
  is_valid_energy,
  is_valid_duration,
  COUNT(*) AS rows
FROM ev_charging_demo.silver.charging_sessions_clean
GROUP BY is_valid_energy, is_valid_duration
ORDER BY rows DESC;
```

## Validated results

- Bronze records: `20,142`
- Silver records: `20,142`
- Valid records: `18,008`
- Invalid energy records: `2,133`
- Invalid duration records: `2,053`
- Missing station records: `0`
- Duplicate records: `0`
- Valid record rate: `89.4%`

Rows with invalid energy or duration remain available in Silver for audit but
are excluded from Gold KPI tables.

## References

- Data-quality notebook: [notebook/06_data_quality_checks.sql](../../notebook/06_data_quality_checks.sql)
- Gold notebook: [notebook/05_build_gold.sql](../../notebook/05_build_gold.sql)
- Original task doc: [docs/05_data_quality.md](../05_data_quality.md)