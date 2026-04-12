import os
import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
from datetime import date


app = FastAPI(title="Vol Surface API", version="0.1.0")

DB_PATH = os.getenv("DUCKDB_PATH", "/data/duckdb/vol_surface.db")


@app.get("/health")
def health_check():
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        conn.execute("SELECT 1")
        conn.close()
        return {"status": "healthy", "db": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


@app.get("/tickers")
def get_tickers():
    """List all available tickers."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute("SELECT DISTINCT symbol FROM dim_ticker ORDER BY symbol").df()
        conn.close()
        return JSONResponse(content=df['symbol'].tolist())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/snapshots/{ticker}")
def get_snapshots(ticker: str):
    """List all snapshot dates available for a ticker."""
    query = """
    SELECT DISTINCT f.snapshot_date
    FROM fact_options_snapshot f
    JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
    WHERE dt.symbol = ?
    ORDER BY f.snapshot_date DESC
    """
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute(query, [ticker]).df()
        conn.close()
        df['snapshot_date'] = df['snapshot_date'].astype(str)
        return JSONResponse(content=df['snapshot_date'].tolist())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/vol-surface/{ticker}")
def get_vol_surface(ticker: str, snapshot_date: Optional[str] = None):
    """Full IV surface: all expiries and strikes for a ticker on a given date."""
    if snapshot_date is None:
        snapshot_date = date.today().isoformat()

    query = """
    SELECT
        f.implied_vol,
        f.last_price,
        f.bid,
        f.ask,
        f.volume,
        f.open_interest,
        f.option_type,
        ds.strike_price,
        de.expiry_date,
        de.days_to_expiry,
        de.expiry_bucket,
        ds.moneyness_bucket
    FROM fact_options_snapshot f
    JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
    JOIN dim_strike ds  ON f.strike_id = ds.strike_id
    JOIN dim_expiry de  ON f.expiry_id = de.expiry_id
    WHERE dt.symbol = ? AND f.snapshot_date = ?
    ORDER BY de.expiry_date, ds.strike_price
    """
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute(query, [ticker, snapshot_date]).df()
        conn.close()
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data for {ticker} on {snapshot_date}")
        df['expiry_date'] = df['expiry_date'].astype(str)
        return JSONResponse(content=df.to_dict(orient='records'))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/term-structure/{ticker}")
def get_term_structure(
    ticker: str,
    snapshot_date: Optional[str] = None,
    expiry_bucket: Optional[str] = None,
):
    """ATM IV by expiry bucket for a ticker."""
    if snapshot_date is None:
        snapshot_date = date.today().isoformat()

    query = """
    SELECT
        de.expiry_bucket,
        de.expiry_date,
        de.days_to_expiry,
        AVG(f.implied_vol) AS avg_iv
    FROM fact_options_snapshot f
    JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
    JOIN dim_strike ds  ON f.strike_id = ds.strike_id
    JOIN dim_expiry de  ON f.expiry_id = de.expiry_id
    WHERE dt.symbol = ?
      AND f.snapshot_date = ?
      AND f.implied_vol IS NOT NULL
      AND ds.moneyness_bucket = 'ATM'
    """
    params = [ticker, snapshot_date]

    if expiry_bucket:
        query += " AND de.expiry_bucket = ?"
        params.append(expiry_bucket)

    query += " GROUP BY de.expiry_bucket, de.expiry_date, de.days_to_expiry ORDER BY de.days_to_expiry"

    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute(query, params).df()
        conn.close()
        df['expiry_date'] = df['expiry_date'].astype(str)
        return JSONResponse(content=df.to_dict(orient='records'))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/vol-smile/{ticker}/{expiry}")
def get_vol_smile(ticker: str, expiry: str, snapshot_date: Optional[str] = None):
    """IV across strikes for a single expiry (vol smile/skew)."""
    if snapshot_date is None:
        snapshot_date = date.today().isoformat()

    query = """
    SELECT
        ds.strike_price,
        ds.moneyness_bucket,
        f.option_type,
        f.implied_vol,
        f.last_price,
        f.bid,
        f.ask
    FROM fact_options_snapshot f
    JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
    JOIN dim_strike ds  ON f.strike_id = ds.strike_id
    JOIN dim_expiry de  ON f.expiry_id = de.expiry_id
    WHERE dt.symbol = ?
      AND f.snapshot_date = ?
      AND de.expiry_date = ?
      AND f.implied_vol IS NOT NULL
    ORDER BY ds.strike_price
    """
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        df = conn.execute(query, [ticker, snapshot_date, expiry]).df()
        conn.close()
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data for {ticker} expiry {expiry}")
        return JSONResponse(content=df.to_dict(orient='records'))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
