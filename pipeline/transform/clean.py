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

    required_cols = ['ticker', 'expiry_date', 'strike', 'option_type', 'last_price', 'underlying_price']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    df['strike'] = pd.to_numeric(df['strike'], errors='coerce')
    df['last_price'] = pd.to_numeric(df['last_price'], errors='coerce')
    df['bid'] = pd.to_numeric(df['bid'], errors='coerce')
    df['ask'] = pd.to_numeric(df['ask'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)
    df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce').fillna(0).astype(int)
    df['underlying_price'] = pd.to_numeric(df['underlying_price'], errors='coerce')
    df['expiry_date'] = pd.to_datetime(df['expiry_date'])

    df = df.dropna(subset=['strike', 'last_price', 'underlying_price'])

    df = df[df['last_price'] > 0]
    df = df[df['strike'] > 0]
    df = df[df['underlying_price'] > 0]

    df['mid_price'] = (df['bid'].fillna(0) + df['ask'].fillna(0)) / 2
    df.loc[df['mid_price'] == 0, 'mid_price'] = df.loc[df['mid_price'] == 0, 'last_price']

    df['option_type'] = df['option_type'].str.lower().str.strip()
    df = df[df['option_type'].isin(['call', 'put'])]

    return df.reset_index(drop=True)


def add_moneyness(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add moneyness bucket (ITM/ATM/OTM) based on strike vs underlying price.
    Direction-aware: ITM/OTM meaning differs for calls vs puts.

    Args:
        df: Options DataFrame with underlying_price column

    Returns:
        DataFrame with moneyness_bucket column
    """
    df = df.copy()

    moneyness = df['strike'] / df['underlying_price']

    conditions = [
        moneyness < 0.95,
        moneyness > 1.05,
    ]

    call_labels = ['ITM', 'OTM']
    put_labels  = ['OTM', 'ITM']

    df['moneyness_bucket'] = 'ATM'

    is_call = df['option_type'] == 'call'
    is_put  = df['option_type'] == 'put'

    for cond, call_val, put_val in zip(conditions, call_labels, put_labels):
        df.loc[cond & is_call, 'moneyness_bucket'] = call_val
        df.loc[cond & is_put,  'moneyness_bucket'] = put_val

    return df


def add_expiry_buckets(df: pd.DataFrame, reference_date: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Add expiry bucket (near/mid/far) and days_to_expiry based on expiry date.
    Filters out expired or same-day options.

    Args:
        df: Options DataFrame
        reference_date: Reference date (defaults to today)

    Returns:
        DataFrame with days_to_expiry and expiry_bucket columns
    """
    df = df.copy()

    if reference_date is None:
        reference_date = pd.Timestamp.now().normalize()

    df['days_to_expiry'] = (df['expiry_date'] - reference_date).dt.days

    df = df[df['days_to_expiry'] > 0]

    df['expiry_bucket'] = pd.cut(
        df['days_to_expiry'],
        bins=[0, 30, 90, float('inf')],
        labels=['near', 'mid', 'far']
    )

    df['expiry_bucket'] = df['expiry_bucket'].astype(str)

    return df.reset_index(drop=True)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '.')
    from ingestion.fetch_options import fetch_options_chain

    df = fetch_options_chain("SPY")
    cleaned = clean_options_data(df)
    cleaned = add_moneyness(cleaned)
    cleaned = add_expiry_buckets(cleaned)
    print(f"Cleaned {len(cleaned)} records")
    print(cleaned[['ticker', 'strike', 'option_type', 'moneyness_bucket',
                    'days_to_expiry', 'expiry_bucket', 'mid_price']].head(10))
