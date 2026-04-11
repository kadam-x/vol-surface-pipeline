import os
import duckdb
import pandas as pd
from typing import Optional
from datetime import datetime


class DuckDBLoader:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("DUCKDB_PATH", "/data/warehouse/vol_surface.db")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_schema()
    
    def _init_schema(self):
        """Initialize star schema tables if they don't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_ticker (
                ticker_id INTEGER PRIMARY KEY,
                symbol VARCHAR,
                sector VARCHAR,
                exchange VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_expiry (
                expiry_id INTEGER PRIMARY KEY,
                expiry_date DATE,
                days_to_expiry INTEGER,
                expiry_bucket VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_strike (
                strike_id INTEGER PRIMARY KEY,
                strike_price DOUBLE,
                moneyness_bucket VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_options_snapshot (
                snapshot_id BIGINT,
                ticker_id INTEGER REFERENCES dim_ticker(ticker_id),
                expiry_id INTEGER REFERENCES dim_expiry(expiry_id),
                strike_id INTEGER REFERENCES dim_strike(strike_id),
                option_type VARCHAR,
                implied_vol DOUBLE,
                last_price DOUBLE,
                bid DOUBLE,
                ask DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                risk_free_rate DOUBLE,
                snapshot_date DATE,
                PRIMARY KEY (snapshot_id, ticker_id, expiry_id, strike_id, option_type)
            )
        """)
    
    def get_or_create_ticker_id(self, symbol: str, sector: Optional[str] = None, exchange: Optional[str] = None) -> int:
        """Get existing ticker_id or create new one."""
        existing = self.conn.execute(
            "SELECT ticker_id FROM dim_ticker WHERE symbol = ?", [symbol]
        ).fetchone()
        
        if existing:
            return existing[0]
        
        max_id = self.conn.execute("SELECT COALESCE(MAX(ticker_id), 0) FROM dim_ticker").fetchone()[0]
        new_id = max_id + 1
        
        self.conn.execute(
            "INSERT INTO dim_ticker (ticker_id, symbol, sector, exchange) VALUES (?, ?, ?, ?)",
            [new_id, symbol, sector, exchange]
        )
        
        return new_id
    
    def get_or_create_expiry_id(self, expiry_date: pd.Timestamp, days_to_expiry: int, expiry_bucket: str) -> int:
        """Get existing expiry_id or create new one."""
        date_str = expiry_date.strftime('%Y-%m-%d')
        
        existing = self.conn.execute(
            "SELECT expiry_id FROM dim_expiry WHERE expiry_date = ?", [date_str]
        ).fetchone()
        
        if existing:
            return existing[0]
        
        max_id = self.conn.execute("SELECT COALESCE(MAX(expiry_id), 0) FROM dim_expiry").fetchone()[0]
        new_id = max_id + 1
        
        self.conn.execute(
            "INSERT INTO dim_expiry (expiry_id, expiry_date, days_to_expiry, expiry_bucket) VALUES (?, ?, ?, ?)",
            [new_id, date_str, days_to_expiry, expiry_bucket]
        )
        
        return new_id
    
    def get_or_create_strike_id(self, strike_price: float, moneyness_bucket: str) -> int:
        """Get existing strike_id or create new one."""
        existing = self.conn.execute(
            "SELECT strike_id FROM dim_strike WHERE strike_price = ?", [strike_price]
        ).fetchone()
        
        if existing:
            return existing[0]
        
        max_id = self.conn.execute("SELECT COALESCE(MAX(strike_id), 0) FROM dim_strike").fetchone()[0]
        new_id = max_id + 1
        
        self.conn.execute(
            "INSERT INTO dim_strike (strike_id, strike_price, moneyness_bucket) VALUES (?, ?, ?)",
            [new_id, strike_price, moneyness_bucket]
        )
        
        return new_id
    
    def upsert_facts(self, df: pd.DataFrame, snapshot_date: str, snapshot_id: int):
        """Upsert fact records, replacing existing data for this snapshot."""
        self.conn.execute("DELETE FROM fact_options_snapshot WHERE snapshot_date = ?", [snapshot_date])
        
        self.conn.register("fact_df", df)
        self.conn.execute("""
            INSERT INTO fact_options_snapshot 
            SELECT 
                snapshot_id, ticker_id, expiry_id, strike_id, option_type,
                implied_vol, last_price, bid, ask, volume, open_interest,
                risk_free_rate, snapshot_date
            FROM fact_df
        """)
        self.conn.unregister("fact_df")
    
    def load_options_data(
        self,
        df: pd.DataFrame,
        snapshot_date: Optional[str] = None,
        risk_free_rate: float = 0.045,
    ):
        """
        Load options data into star schema.
        
        Args:
            df: Options DataFrame with implied_vol computed
            snapshot_date: Date for this snapshot (defaults to today)
            risk_free_rate: Risk-free rate used for IV calculation
        """
        if snapshot_date is None:
            snapshot_date = datetime.now().strftime('%Y-%m-%d')
        
        snapshot_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))
        
        records = []
        for _, row in df.iterrows():
            ticker_id = self.get_or_create_ticker_id(row['ticker'])
            expiry_id = self.get_or_create_expiry_id(
                row['expiry_date'], row.get('days_to_expiry', 0), row.get('expiry_bucket', 'mid')
            )
            strike_id = self.get_or_create_strike_id(
                row['strike'], row.get('moneyness_bucket', 'ATM')
            )
            
            records.append({
                'snapshot_id': snapshot_id,
                'ticker_id': ticker_id,
                'expiry_id': expiry_id,
                'strike_id': strike_id,
                'option_type': row['option_type'],
                'implied_vol': row.get('implied_vol'),
                'last_price': row['last_price'],
                'bid': row.get('bid'),
                'ask': row.get('ask'),
                'volume': row.get('volume', 0),
                'open_interest': row.get('open_interest', 0),
                'risk_free_rate': risk_free_rate,
                'snapshot_date': snapshot_date,
            })
        
        fact_df = pd.DataFrame(records)
        
        self.upsert_facts(fact_df, snapshot_date, snapshot_id)
        
        print(f"Loaded {len(fact_df)} records into warehouse")
    
    def close(self):
        self.conn.close()


if __name__ == "__main__":
    loader = DuckDBLoader()
    
    test_df = pd.DataFrame({
        'ticker': ['SPY', 'SPY'],
        'expiry_date': pd.to_datetime(['2025-06-20', '2025-06-20']),
        'strike': [500.0, 510.0],
        'option_type': ['call', 'call'],
        'last_price': [5.0, 2.5],
        'bid': [4.8, 2.3],
        'ask': [5.2, 2.7],
        'implied_vol': [0.15, 0.16],
        'days_to_expiry': [30, 30],
        'expiry_bucket': ['near', 'near'],
        'moneyness_bucket': ['OTM', 'ATM'],
    })
    
    loader.load_options_data(test_df)
    print("DuckDB loader ready")