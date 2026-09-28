# 🏗️ Modern Data Stack — Orchestrated Analytics Engineering

How analytics actually runs in companies: raw data flows through **staging** and
**mart** models, every step is **tested**, and a **DAG orchestrator** runs it all
in dependency order. This builds the whole dbt + Airflow + Great-Expectations
pattern from scratch on a local SQLite warehouse — no external services.

## What this project demonstrates
- **DAG orchestration** — tasks declare upstream dependencies; the scheduler
  topologically sorts them, runs each after its parents succeed, records status
  and timing, and skips downstream tasks when an upstream one fails.
- **dbt-style SQL models** — layered transformations: `stg_*` staging models
  clean the raw tables (dedupe, coalesce nulls, cast types, filter invalid
  rows); `mart_*` models aggregate them into business tables.
- **Data-quality tests as gates** — not-null, unique, accepted-values,
  referential-integrity, and row-count checks run *between* layers; failures
  fail the run instead of silently corrupting a dashboard.
- **Lineage + run report** — the DAG is plotted and a JSON report captures
  execution order, timings, and every test result.

## Demo

```text
$ python3 src/pipeline.py
DAG order: ingest -> test_raw -> stg_customers -> stg_orders -> test_staging
           -> mart_customer_revenue -> mart_region_summary -> test_marts
raw data issues caught: 3 | staging/marts tests: all enforced & passed

Revenue by region:
 region  n_customers  revenue
   West           58 32648.89
  North           50 30783.54
   East           46 26317.80
  South           33 21216.31
Unknown           13  6839.96
```

The raw-data profile catches the injected duplicate, nulls, and orphan keys; the
staging models clean them (nulls become `Unknown`), and the downstream contracts
pass. `reports/lineage.png` shows the pipeline DAG.

## Components

| Piece | Where | Idea |
|---|---|---|
| Orchestrator | `src/orchestrator.py` | DAG, toposort, run, skip-on-failure |
| Models | `src/pipeline.py` (MODELS) | dbt-style staging + mart SQL |
| Tests | `src/quality.py` | not_null / unique / accepted_values / relationships |
| Pipeline | `src/pipeline.py` | wires the DAG, runs it, writes the report |

## Project structure
```
modern-data-stack-orchestration/
├── data/                  # raw_*.csv, warehouse.db (generated)
├── src/
│   ├── generate_data.py   # dirty raw sources
│   ├── orchestrator.py    # DAG scheduler
│   ├── quality.py         # data-quality tests
│   └── pipeline.py        # models + run + lineage
├── reports/               # lineage.png, run_report.json
├── requirements.txt
├── torun.txt
└── license.md
```

## Run it
```bash
./run.sh        # or see torun.txt
```

**Production swap**: dbt for the SQL models, Airflow or Dagster for orchestration
and scheduling, and Great Expectations for the test suite — on Snowflake,
BigQuery, or DuckDB. The structure (staging → marts, tests as gates, a DAG) is
identical.
