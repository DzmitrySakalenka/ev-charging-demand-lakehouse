# Current issues and follow-ups

This file tracks known issues and deferred improvements after the implemented
demo pipeline. These are not blockers for the demo, but they are useful next
steps if the project is extended.

## AFDC station matching is partial

Silver currently matches sessions to AFDC metadata by normalized station name.
That matches `3,946` sessions and leaves `16,196` unmatched.

Follow-up:

- Add address-based fallback matching between Cary session addresses and AFDC
  station addresses.
- Keep the current `station_match_status` column and add clearer statuses such
  as `matched_by_name`, `matched_by_address`, and `unmatched`.

## Utilization is a proxy

`gold.station_utilization_proxy` ranks station demand from sessions and kWh. It
does not measure true charger occupancy or port utilization.

Follow-up:

- Keep the table name and dashboard wording explicit.
- Add connector-level or port-occupancy data only if a reliable source becomes
  available.

## One weather join is missing

One session at `2022-11-06T06:44:22Z` does not join to weather. This falls on
the `America/New_York` daylight-saving fallback day.

Follow-up:

- Investigate local-to-UTC timestamp handling around DST fallback.
- Decide whether to accept the one-row gap or apply a specific fallback rule
  for ambiguous local weather hours.

## Batch ingestion uses overwrite

The production Bronze, Silver, and Gold notebooks rebuild their target tables
with overwrite semantics. This is acceptable for the demo but not a full
incremental production design.

Follow-up:

- Promote the Auto Loader notebooks into the job graph after adding checkpoint
  tests and a file-versioning policy.
- Avoid overwriting raw filenames when demonstrating incremental ingestion.

## Auto Loader variants are prototypes

The Auto Loader notebooks exist for comparison, but they are deliberately not on
the main job graph.

Follow-up:

- Decide whether Auto Loader should replace manual Bronze ingestion.
- Standardize checkpoint and schema locations under a dedicated state path.
- Keep the binary-file parsing pattern for Cary and AFDC unless tested
  alternatives handle the source file quirks correctly.

## AFDC runtime dependency is implicit

`notebook/03_ingest_afdc_stations.py` uses `requests`, which is available in
Databricks Runtime but is not currently listed as a project dependency.

Follow-up:

- Add `requests` to project dependencies if job environments are later derived
  from `pyproject.toml`.

## Shared column helpers are duplicated

Several notebooks define small normalization helpers such as `_sanitize` or
`_normalize_text`.

Follow-up:

- Move reusable helper functions into `src/` only if the project grows into a
  packaged library.
- Restore the removed bundle wheel artifact only after real package code exists.

## Lakeflow expectations not implemented

Data quality is implemented with Silver flags and a Gold summary table. Lakeflow
Declarative Pipeline expectations are not part of the MVP.

Follow-up:

- Add Lakeflow expectations only if the project becomes a Lakeflow-focused demo.
