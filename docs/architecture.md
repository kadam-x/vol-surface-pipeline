# Architecture Documentation

## Overview

The Implied Volatility Surface Pipeline is a market data infrastructure system that:

1. **Ingests** equity options data (yfinance) and macro rates (FRED API / fallback)
2. **Stores** raw data in MinIO (S3-compatible) as Parquet
3. **Transforms** data with Black-Scholes IV calculation
4. **Loads** into DuckDB star schema
5. **Serves** analytics via FastAPI and Grafana
6. **Orchestrates** via Apache Airflow DAG

## Data Flow

```
[yfinance] ─────┐
                ├──► [MinIO: raw/] ──► [Transform] ──► [DuckDB: star schema]
[FRED API] ─────┘                                           │
                                                             ▼
                                                    [FastAPI] / [Grafana]
```

## Components

### Ingestion Layer (`pipeline/ingestion/`)

| Component | Description |
|-----------|-------------|
| `fetch_options.py` | Pulls SPY options chain via yfinance (calls + puts) |
| `fetch_macro.py` | Fetches 10Y Treasury rate; falls back to 4.5% if no API key |

### Storage Layer (`pipeline/storage/`)

| Component | Description |
|-----------|-------------|
| `minio_client.py` | Uploads/downloads Parquet to MinIO bucket |

### Transform Layer (`pipeline/transform/`)

| Component | Description |
|-----------|-------------|
| `clean.py` | Null handling, type casting, adds moneyness/expiry buckets |
| `iv_surface.py` | Black-Scholes price + brentq IV solver |
| `load_duckdb.py` | Star schema create + upsert facts |

### Serving Layer (`pipeline/serving/`)

| Component | Description |
|-----------|-------------|
| `api.py` | FastAPI with 4 endpoints: vol-surface, term-structure, vol-smile, health |

### Orchestration (`airflow/dags/`)

| Task | Description |
|------|-------------|
| `fetch_options_chain` | Download options data |
| `fetch_macro_rates` | Get risk-free rate |
| `upload_raw_to_minio` | Store raw Parquet |
| `transform_and_compute_iv` | Clean + calculate IV |
| `load_duckdb` | Insert into warehouse |
| `validate_data_quality` | Check IV success rate |

## Star Schema

```
┌─────────────────────┐     ┌─────────────────────┐
│   dim_ticker        │     │   dim_expiry        │
├─────────────────────┤     ├─────────────────────┤
│ ticker_id (PK)      │     │ expiry_id (PK)      │
│ symbol              │     │ expiry_date         │
│ sector              │     │ days_to_expiry      │
│ exchange            │     │ expiry_bucket       │
└─────────┬───────────┘     └──────────┬──────────┘
          │                             │
          │    ┌─────────────────────────┴────────────┐
          │    │                                      │
          ▼    ▼                                      ▼
┌──────────────────────────────────────────────────────────┐
│              fact_options_snapshot                       │
├──────────────────────────────────────────────────────────┤
│ snapshot_id (PK)                                         │
│ ticker_id (FK) ──────────────────────────────────────────►
│ expiry_id (FK)  ─────────────────────────────────────────►
│ strike_id (FK)  ─────────────────────────────────────────►
│ option_type     │                                      │
│ implied_vol     │                                      │
│ last_price      │     ┌──────────────────────────────┐ │
│ bid             │     │   dim_strike                  │ │
│ ask             └─────►├──────────────────────────────┤ │
│ volume          │     │ strike_id (PK)                │ │
│ open_interest   │     │ strike_price                  │ │
│ risk_free_rate  │     │ moneyness_bucket              │ │
│ snapshot_date   │     └──────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/vol-surface/{ticker}` | Full IV surface |
| GET | `/term-structure/{ticker}` | ATM IV by expiry |
| GET | `/vol-smile/{ticker}/{expiry}` | IV across strikes |
| GET | `/tickers` | List available tickers |

## Configuration

All settings via `.env`:

```bash
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
DUCKDB_PATH=/data/warehouse/vol_surface.db
TARGET_TICKERS=SPY
RISK_FREE_RATE_FALLBACK=0.045
```

## Deployment

```bash
docker compose up -d
```

Services:
- **MinIO**: http://localhost:9000 (console :9001)
- **Airflow**: http://localhost:8080 (admin/admin)
- **FastAPI**: http://localhost:8000
- **Grafana**: http://localhost:3000 (admin/admin)