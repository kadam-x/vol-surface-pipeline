# Implied Volatility Surface Construction Pipeline — PLANNING.md

## Overview

An end-to-end market risk data pipeline that ingests equity options chain data and
macro rates, computes implied volatility surfaces via Black-Scholes inversion, stores
results in a star-schema data warehouse, and serves analytics through a REST API and
time-series dashboard. Designed to mirror production-grade market data infrastructure.

---

## Goals

- Ingest live/daily equity options data and risk-free rate from two distinct sources
- Store raw data in an S3-compatible landing zone (MinIO) as Parquet
- Compute per-strike, per-expiry implied volatility using numerical BS inversion
- Load transformed data into a star-schema DuckDB warehouse
- Orchestrate the full pipeline with Apache Airflow (idempotent, schedulable DAG)
- Serve vol surface analytics via FastAPI and visualise via Grafana

---

## Data Sources

| Source | Data | Format | Method |
|--------|------|--------|--------|
| yfinance | Equity options chain (calls + puts, strike, expiry, bid/ask, last price) | JSON (semi-structured) | Python client, batch pull |
| FRED API | US Treasury yield curve (risk-free rate proxy) | JSON | REST API, daily |

---

## Architecture

```
[yfinance]──┐
            ├──► [MinIO raw landing zone] ──► [Transform: clean + IV calc] ──► [DuckDB warehouse]
[FRED API]──┘                                                                        │
                                                                               [FastAPI]
                                                                               [Grafana]
```

### Layers

1. **Ingestion** — fetch options chain + macro data, write raw Parquet to MinIO
2. **Transform** — null handling, type casting, Black-Scholes inversion (SciPy brentq),
   compute IV per (ticker, expiry, strike, option_type)
3. **Load** — insert into DuckDB star schema
4. **Serve** — FastAPI exposes analytical endpoints; Grafana reads DuckDB directly

---

## Data Model (Star Schema)

```
fact_options_snapshot
  snapshot_id       BIGINT PK
  ticker_id         INT FK → dim_ticker
  expiry_id         INT FK → dim_expiry
  strike_id         INT FK → dim_strike
  option_type       VARCHAR  -- 'call' | 'put'
  implied_vol       DOUBLE
  last_price        DOUBLE
  bid               DOUBLE
  ask               DOUBLE
  volume            INT
  open_interest     INT
  risk_free_rate    DOUBLE
  snapshot_date     DATE

dim_ticker
  ticker_id         INT PK
  symbol            VARCHAR
  sector            VARCHAR
  exchange          VARCHAR

dim_expiry
  expiry_id         INT PK
  expiry_date       DATE
  days_to_expiry    INT
  expiry_bucket     VARCHAR  -- 'near' | 'mid' | 'far'

dim_strike
  strike_id         INT PK
  strike_price      DOUBLE
  moneyness_bucket  VARCHAR  -- 'ITM' | 'ATM' | 'OTM'
```

---

## Airflow DAG

**DAG id:** `options_vol_surface_daily`
**Schedule:** `0 18 * * 1-5` (18:00 UTC, weekdays)
**Idempotency:** all tasks are safe to re-run; DuckDB upserts on snapshot_date

### Tasks

```
fetch_options_chain
       │
fetch_macro_rates
       │
upload_raw_to_minio
       │
transform_and_compute_iv
       │
load_duckdb
       │
validate_data_quality
```

---

## Analytical Queries (documented, minimum 3)

1. **ATM vol time series** — track at-the-money IV per ticker over time
2. **Vol smile** — IV across strikes for a single expiry (smile/skew shape)
3. **Cross-ticker vol comparison** — compare 30-day ATM IV across multiple tickers
4. **Term structure** — IV by expiry bucket for a given ticker on a given date

---

## API Endpoints (FastAPI)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/vol-surface/{ticker}` | Full IV surface (all expiries, all strikes) |
| GET | `/term-structure/{ticker}` | ATM IV by expiry bucket |
| GET | `/vol-smile/{ticker}/{expiry}` | IV across strikes for a given expiry |
| GET | `/health` | Pipeline health check |

---

## Infrastructure

- **Docker Compose services:** airflow, minio, duckdb (file mount), fastapi, grafana
- **Config:** all secrets and paths via `.env` (no hardcoded values)
- **Reproducibility:** `docker compose up` starts full environment from scratch

---

## Directory Structure

```
implied-vol-surface/
├── docker-compose.yml
├── .env.example
├── README.md
├── docs/
│   ├── architecture.md
│   └── star_schema.png
├── airflow/
│   └── dags/
│       └── options_pipeline_dag.py
├── pipeline/
│   ├── ingestion/
│   │   ├── fetch_options.py
│   │   └── fetch_macro.py
│   ├── storage/
│   │   └── minio_client.py
│   ├── transform/
│   │   ├── clean.py
│   │   ├── iv_surface.py
│   │   └── load_duckdb.py
│   ├── serving/
│   │   └── api.py
│   └── utils/
│       └── config.py
├── grafana/
│   └── dashboards/
│       └── vol_surface.json
├── queries/
│   ├── atm_vol_timeseries.sql
│   ├── vol_smile.sql
│   └── cross_ticker_comparison.sql
└── tests/
    └── test_iv_surface.py
```

---

## Implementation Order

1. Set up Docker Compose (MinIO + DuckDB + Airflow skeleton)
2. Implement `fetch_options.py` and `fetch_macro.py` — verify raw data shape
3. Implement `minio_client.py` — raw Parquet upload
4. Implement `clean.py` + `iv_surface.py` — core IV computation
5. Implement `load_duckdb.py` — star schema creation and upsert logic
6. Wire Airflow DAG with all tasks and test idempotency
7. Implement FastAPI endpoints
8. Configure Grafana dashboard
9. Write SQL queries and document results
10. Write README and architecture doc
