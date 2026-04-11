import yfinance as yf
import pandas as pd
from datetime import datetime
from typing import Optional
import os


def fetch_options_chain(ticker: str, expiry_filter: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch options chain data for a given ticker.
    
    Args:
        ticker: Stock symbol (e.g., 'SPY')
        expiry_filter: Optional number of near-term expiries to fetch
    
    Returns:
        DataFrame with columns: ticker, expiry_date, strike, option_type,
                                last_price, bid, ask, volume, open_interest
    """
    yf_ticker = yf.Ticker(ticker)
    
    expirations = yf_ticker.options
    if not expirations:
        raise ValueError(f"No options data available for {ticker}")
    
    if expiry_filter:
        expirations = expirations[:expiry_filter]
    
    records = []
    for exp_date in expirations:
        try:
            opt = yf_ticker.option_chain(exp_date)
            
            for df, opt_type in [(opt.calls, 'call'), (opt.puts, 'put')]:
                if df is None or df.empty:
                    continue
                
                for _, row in df.iterrows():
                    records.append({
                        'ticker': ticker,
                        'expiry_date': pd.to_datetime(exp_date),
                        'strike': row.get('strike'),
                        'option_type': opt_type,
                        'last_price': row.get('lastPrice'),
                        'bid': row.get('bid'),
                        'ask': row.get('ask'),
                        'volume': row.get('volume'),
                        'open_interest': row.get('openInterest'),
                    })
        except Exception as e:
            print(f"Warning: Failed to fetch options for {exp_date}: {e}")
            continue
    
    df = pd.DataFrame(records)
    
    if df.empty:
        raise ValueError(f"No options data retrieved for {ticker}")
    
    return df


def fetch_underlying_price(ticker: str) -> float:
    """Fetch current underlying price for the ticker."""
    yf_ticker = yf.Ticker(ticker)
    info = yf_ticker.info
    return info.get('currentPrice') or info.get('regularMarketPrice')


if __name__ == "__main__":
    tickers = os.getenv("TARGET_TICKERS", "SPY").split(",")
    
    for ticker in tickers:
        print(f"Fetching options for {ticker}...")
        df = fetch_options_chain(ticker.strip())
        print(f"Retrieved {len(df)} option records")
        print(df.head())