# Implied Volatility Surface Pipeline

End-to-end market data pipeline that constructs implied volatility surfaces from equity options data, stores in a DuckDB star schema warehouse, and serves analytics via FastAPI and Grafana.

## Quick Start

```bash
# Copy environment template
cp .env.example .env

# Start all services
docker compose up -d
```

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO | http://localhost:9000 | minioadmin / minioadmin |
| Airflow | http://localhost:8080 | admin / admin |
| FastAPI | http://localhost:8000 | - |
| Grafana | http://localhost:3000 | admin / admin |

## Pipeline Flow

```
yfinance (options) ─┐
                    ├──► MinIO (raw) ──► Transform (IV calc) ──► DuckDB
FRED API (rates) ────┘                                           │
                                                                  ▼
                                                          FastAPI / Grafana
```

## API Endpoints

- `GET /health` — Health check
- `GET /vol-surface/{ticker}` — Full IV surface
- `GET /term-structure/{ticker}` — ATM IV by expiry bucket
- `GET /vol-smile/{ticker}/{expiry}` — Vol smile (IV across strikes)
- `GET /tickers` — Available tickers

## SQL Queries

See `queries/` directory for analytical queries:
- `atm_vol_timeseries.sql` — ATM IV over time
- `vol_smile.sql` — IV across strikes for single expiry
- `cross_ticker_comparison.sql` — Compare IV across tickers
- `term_structure.sql` — IV by expiry bucket

## Configuration

Edit `.env` to configure:
- `TARGET_TICKERS` — Comma-separated tickers (default: SPY)
- `RISK_FREE_RATE_FALLBACK` — Fallback rate when no FRED API key
- `DUCKDB_PATH` — Path to DuckDB file

## Development

```bash
# Install dependencies
uv sync

# Run tests
pytest tests/

# Run pipeline locally
python pipeline/ingestion/fetch_options.py
```