# EV Charging Demand Lakehouse — Databricks Implementation Plan

This repository starter contains a step-by-step implementation plan for a compact Databricks data engineering project focused on EV charging demand analytics.

The project is designed to be small enough to build quickly, but complete enough to demonstrate practical Databricks and data engineering skills:

- Medallion architecture: Bronze → Silver → Gold
- Public data/API ingestion
- Delta tables
- Data quality checks
- Enrichment with weather and station metadata
- SQL analytics and dashboard-ready gold tables
- A clear handoff/presentation narrative

## Recommended project scope

**Project name:** EV Charging Demand Lakehouse  
**Core data:** Town of Cary EV charging sessions  
**Enrichment:** Open-Meteo historical weather; optional AFDC/NLR station metadata  
**Main business question:** Where and when does EV charging demand peak, and what station-level KPIs can support operational decisions?

## How to use this starter kit

1. Start with [`PLAN.md`](PLAN.md).
2. Follow the detailed task docs in order under [`docs/`](docs/).
3. Use [`notebook_skeletons/`](notebook_skeletons/) as starting points for Databricks notebooks.
4. Copy the files under [`templates/`](templates/) into your final repo if you want polished documentation.

## Suggested final repo structure

```text
ev-charging-demand-lakehouse/
  README.md
  PLAN.md
  docs/
  notebooks/
  sql/
  dashboard/
```

## Expected final deliverables

- Bronze Delta tables for raw sessions, weather, and optional station metadata
- Silver Delta tables for cleaned/enriched sessions
- Gold Delta tables for station KPIs, hourly demand, and data quality summary
- A Databricks Job / Lakeflow Job showing task orchestration
- Dashboard or SQL queries showing business insights
- Short README explaining architecture, assumptions, limitations, and next steps
