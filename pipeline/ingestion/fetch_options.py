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

    underlying_price = fetch_underlying_price(ticker) 
    
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
                        'mid_price': (row.get('bid', 0) + row.get('ask', 0)) / 2,
                        'underlying_price': underlying_price,
                    })
        except Exception as e:
            print(f"Warning: Failed to fetch options for {exp_date}: {e}")
            continue
    
    df = pd.DataFrame(records)
    
    if df.empty:
        raise ValueError(f"No options data retrieved for {ticker}")
    
    return df


def fetch_underlying_price(ticker: str) -> float:
    yf_ticker = yf.Ticker(ticker)
    info = yf_ticker.info
    price = info.get('currentPrice') or info.get('regularMarketPrice')
    if price is None:
        raise ValueError(f"Could not fetch underlying price for {ticker}")
    return float(price)

if __name__ == "__main__":
    tickers = os.getenv("TARGET_TICKERS", "SPY").split(",")
    
    for ticker in tickers:
        print(f"Fetching options for {ticker}...")
        df = fetch_options_chain(ticker.strip())
        print(f"Retrieved {len(df)} option records")
        print(df.head())
