import duckdb
import pandas as pd
from datetime import date
from typing import Optional
import os

SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS snapshot_id_seq;

CREATE TABLE IF NOT EXISTS dim_ticker (
    ticker_id   INTEGER PRIMARY KEY,
    symbol      VARCHAR NOT NULL UNIQUE,
    sector      VARCHAR,
    exchange    VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_expiry (
    expiry_id      INTEGER PRIMARY KEY,
    expiry_date    DATE NOT NULL UNIQUE,
    days_to_expiry INTEGER,
    expiry_bucket  VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_strike (
    strike_id        INTEGER PRIMARY KEY,
    strike_price     DOUBLE NOT NULL,
    moneyness_bucket VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_options_snapshot (
    snapshot_id      BIGINT PRIMARY KEY DEFAULT nextval('snapshot_id_seq'),
    ticker_id        INTEGER REFERENCES dim_ticker(ticker_id),
    expiry_id        INTEGER REFERENCES dim_expiry(expiry_id),
    strike_id        INTEGER REFERENCES dim_strike(strike_id),
    option_type      VARCHAR NOT NULL,
    implied_vol      DOUBLE,
    last_price       DOUBLE,
    mid_price        DOUBLE,
    bid              DOUBLE,
    ask              DOUBLE,
    volume           INTEGER,
    open_interest    INTEGER,
    underlying_price DOUBLE,
    risk_free_rate   DOUBLE,
    snapshot_date    DATE NOT NULL
);
"""


def get_connection(duckdb_path: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    path = duckdb_path or os.getenv("DUCKDB_PATH", "/data/duckdb/vol_surface.db")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return duckdb.connect(path)


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables and sequences if they do not exist."""
    conn.execute(SCHEMA_SQL)
    print("Schema initialised")


def _upsert_dim_ticker(conn: duckdb.DuckDBPyConnection, symbols: list[str]) -> dict[str, int]:
    for symbol in symbols:
        existing = conn.execute(
            "SELECT ticker_id FROM dim_ticker WHERE symbol = ?", [symbol]
        ).fetchone()
        if not existing:
            next_id = conn.execute(
                "SELECT COALESCE(MAX(ticker_id), 0) + 1 FROM dim_ticker"
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO dim_ticker (ticker_id, symbol) VALUES (?, ?)",
                [next_id, symbol]
            )

    result = conn.execute("SELECT symbol, ticker_id FROM dim_ticker").fetchall()
    return {row[0]: row[1] for row in result}


def _upsert_dim_expiry(conn: duckdb.DuckDBPyConnection, expiry_rows: list[dict]) -> dict:
    for row in expiry_rows:
        existing = conn.execute(
            "SELECT expiry_id FROM dim_expiry WHERE expiry_date = ?",
            [row['expiry_date']]
        ).fetchone()
        if not existing:
            next_id = conn.execute(
                "SELECT COALESCE(MAX(expiry_id), 0) + 1 FROM dim_expiry"
            ).fetchone()[0]
            conn.execute(
                """INSERT INTO dim_expiry (expiry_id, expiry_date, days_to_expiry, expiry_bucket)
                   VALUES (?, ?, ?, ?)""",
                [next_id, row['expiry_date'], row['days_to_expiry'], row['expiry_bucket']]
            )

    result = conn.execute("SELECT expiry_date, expiry_id FROM dim_expiry").fetchall()
    return {row[0]: row[1] for row in result}


def _upsert_dim_strike(conn: duckdb.DuckDBPyConnection, strike_rows: list[dict]) -> dict:
    for row in strike_rows:
        existing = conn.execute(
            "SELECT strike_id FROM dim_strike WHERE strike_price = ? AND moneyness_bucket = ?",
            [row['strike_price'], row['moneyness_bucket']]
        ).fetchone()
        if not existing:
            next_id = conn.execute(
                "SELECT COALESCE(MAX(strike_id), 0) + 1 FROM dim_strike"
            ).fetchone()[0]
            conn.execute(
                """INSERT INTO dim_strike (strike_id, strike_price, moneyness_bucket)
                   VALUES (?, ?, ?)""",
                [next_id, row['strike_price'], row['moneyness_bucket']]
            )

    result = conn.execute(
        "SELECT strike_price, moneyness_bucket, strike_id FROM dim_strike"
    ).fetchall()
    return {(row[0], row[1]): row[2] for row in result}


def delete_existing_snapshot(conn: duckdb.DuckDBPyConnection, snapshot_date: date) -> None:
    """
    Delete all fact rows for a given snapshot date before re-inserting.
    Makes the load step idempotent — re-running for the same date overwrites.
    """
    deleted = conn.execute(
        "DELETE FROM fact_options_snapshot WHERE snapshot_date = ?",
        [snapshot_date]
    ).rowcount
    if deleted:
        print(f"Deleted {deleted} existing rows for {snapshot_date} (idempotent re-run)")


def load_to_duckdb(
    df: pd.DataFrame,
    risk_free_rate: float,
    duckdb_path: Optional[str] = None,
    snapshot_date: Optional[date] = None,
) -> None:
    """
    Load IV surface DataFrame into DuckDB star schema.

    Args:
        df: Output of compute_iv_surface — cleaned, with implied_vol column
        risk_free_rate: Risk-free rate used for IV computation
        duckdb_path: Path to DuckDB file (falls back to DUCKDB_PATH env var)
        snapshot_date: Date to tag rows with (defaults to today)
    """
    if snapshot_date is None:
        snapshot_date = date.today()

    conn = get_connection(duckdb_path)
    init_schema(conn)

    # --- dim_ticker ---
    symbols = df['ticker'].unique().tolist()
    ticker_map = _upsert_dim_ticker(conn, symbols)

    # --- dim_expiry ---
    expiry_df = (
        df[['expiry_date', 'days_to_expiry', 'expiry_bucket']]
        .drop_duplicates(subset=['expiry_date'])
    )
    expiry_rows = expiry_df.to_dict(orient='records')
    expiry_map = _upsert_dim_expiry(conn, expiry_rows)

    # --- dim_strike ---
    strike_df = (
        df[['strike', 'moneyness_bucket']]
        .drop_duplicates()
        .rename(columns={'strike': 'strike_price'})
    )
    strike_rows = strike_df.to_dict(orient='records')
    strike_map = _upsert_dim_strike(conn, strike_rows)

    # --- fact table ---
    delete_existing_snapshot(conn, snapshot_date)

    records = []
    for _, row in df.iterrows():
        records.append({
            'ticker_id':        ticker_map[row['ticker']],
            'expiry_id':        expiry_map[row['expiry_date'].date()
                                           if hasattr(row['expiry_date'], 'date')
                                           else row['expiry_date']],
            'strike_id':        strike_map[(row['strike'], row['moneyness_bucket'])],
            'option_type':      row['option_type'],
            'implied_vol':      row['implied_vol'],
            'last_price':       row.get('last_price'),
            'mid_price':        row.get('mid_price'),
            'bid':              row.get('bid'),
            'ask':              row.get('ask'),
            'volume':           int(row.get('volume', 0)),
            'open_interest':    int(row.get('open_interest', 0)),
            'underlying_price': row['underlying_price'],
            'risk_free_rate':   risk_free_rate,
            'snapshot_date':    snapshot_date,
        })

    fact_df = pd.DataFrame(records)
    conn.execute(
        "INSERT INTO fact_options_snapshot SELECT nextval('snapshot_id_seq'), * FROM fact_df"
    )

    print(f"Loaded {len(fact_df)} rows into fact_options_snapshot for {snapshot_date}")
    conn.close()


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '.')
    from ingestion.fetch_options import fetch_options_chain
    from ingestion.fetch_macro import fetch_risk_free_rate
    from transform.clean import clean_options_data, add_moneyness, add_expiry_buckets
    from transform.iv_surface import compute_iv_surface

    ticker = "SPY"
    print(f"Running full pipeline for {ticker}...")

    raw = fetch_options_chain(ticker)
    cleaned = clean_options_data(raw)
    cleaned = add_moneyness(cleaned)
    cleaned = add_expiry_buckets(cleaned)

    r = fetch_risk_free_rate()
    surface = compute_iv_surface(cleaned, r)

    load_to_duckdb(surface, r)
    print("Done.")
