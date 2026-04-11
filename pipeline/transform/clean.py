import pandas as pd
import numpy as np
from typing import Optional


def clean_options_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean options data: handle nulls, type casting, validation.
    
    Args:
        df: Raw options DataFrame from fetch_options
    
    Returns:
        Cleaned DataFrame ready for IV calculation
    """
    df = df.copy()
    
    required_cols = ['ticker', 'expiry_date', 'strike', 'option_type', 'last_price']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    
    df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
    df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')
    df['bid'] = pd.to_numeric(df['bid'], errors='coerce')
    df['ask'] = pd.to_numeric(df['ask'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)
    df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce').fillna(0).astype(int)
    df['expiry_date'] = pd.to_datetime(df['expiry_date'])
    
    df = df.dropna(subset=['strike', 'last_price'])
    
    df = df[df['last_price'] > 0]
    df = df[df['strike'] > 0]
    
    df['mid_price'] = (df['bid'].fillna(0) + df['ask'].fillna(0)) / 2
    df.loc[df['mid_price'] == 0, 'mid_price'] = df.loc[df['mid_price'] == 0, 'last_price']
    
    df['option_type'] = df['option_type'].str.lower().str.strip()
    df = df[df['option_type'].isin(['call', 'put'])]
    
    return df.reset_index(drop=True)


def add_moneyness(df: pd.DataFrame, spot_price: float) -> pd.DataFrame:
    """
    Add moneyness bucket (ITM/ATM/OTM) based on strike vs spot.
    
    Args:
        df: Options DataFrame
        spot_price: Current underlying price
    
    Returns:
        DataFrame with moneyness_bucket column
    """
    df = df.copy()
    
    moneyness = df['strike'] / spot_price
    
    df['moneyness_bucket'] = pd.cut(
        moneyness,
        bins=[0, 0.95, 1.05, float('inf')],
        labels=['OTM', 'ATM', 'ITM']
    )
    
    df['moneyness_bucket'] = df['moneyness_bucket'].astype(str)
    
    return df


def add_expiry_buckets(df: pd.DataFrame, reference_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Add expiry bucket (near/mid/far) based on days to expiry.
    
    Args:
        df: Options DataFrame
        reference_date: Reference date (defaults to today)
    
    Returns:
        DataFrame with expiry_bucket column
    """
    df = df.copy()
    
    if reference_date is None:
        reference_date = pd.Timestamp.now().normalize()
    
    df['days_to_expiry'] = (df['expiry_date'] - reference_date).dt.days
    
    df['expiry_bucket'] = pd.cut(
        df['days_to_expiry'],
        bins=[-float('inf'), 30, 90, float('inf')],
        labels=['near', 'mid', 'far']
    )
    
    df['expiry_bucket'] = df['expiry_bucket'].astype(str)
    
    return df


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '.')
    from ingestion.fetch_options import fetch_options_chain
    
    df = fetch_options_chain("SPY")
    cleaned = clean_options_data(df)
    print(f"Cleaned {len(cleaned)} records")
    print(cleaned.head())