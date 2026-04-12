import numpy as np
import pandas as pd
from scipy.stats import norm
from scipy.optimize import brentq
from typing import Optional


def black_scholes_price(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
    """
    Compute Black-Scholes theoretical price for a European option.

    Args:
        S: Underlying price
        K: Strike price
        T: Time to expiry in years
        r: Risk-free rate as decimal
        sigma: Volatility as decimal
        option_type: 'call' or 'put'

    Returns:
        Theoretical option price
    """
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    if option_type == 'call':
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def compute_iv(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    option_type: str,
    lower_bound: float = 1e-6,
    upper_bound: float = 20.0,
) -> Optional[float]:
    """
    Compute implied volatility via Brent's method (Black-Scholes inversion).

    Args:
        market_price: Observed market price of the option
        S: Underlying price
        K: Strike price
        T: Time to expiry in years
        r: Risk-free rate as decimal
        option_type: 'call' or 'put'
        lower_bound: Lower sigma bound for root search
        upper_bound: Upper sigma bound for root search

    Returns:
        Implied volatility as decimal, or None if computation fails
    """
    if T <= 0 or market_price <= 0 or S <= 0 or K <= 0:
        return None

    # intrinsic value check — market price must exceed intrinsic value
    if option_type == 'call':
        intrinsic = max(S - K * np.exp(-r * T), 0)
    else:
        intrinsic = max(K * np.exp(-r * T) - S, 0)

    if market_price < intrinsic - 1e-4:
        return None

    objective = lambda sigma: black_scholes_price(S, K, T, r, sigma, option_type) - market_price

    try:
        if objective(lower_bound) * objective(upper_bound) > 0:
            return None

        iv = brentq(objective, lower_bound, upper_bound, xtol=1e-6, maxiter=500)
        return iv if 0 < iv < upper_bound else None

    except (ValueError, RuntimeError):
        return None


def compute_iv_surface(df: pd.DataFrame, risk_free_rate: float) -> pd.DataFrame:
    """
    Compute implied volatility for every option in the DataFrame.

    Args:
        df: Cleaned options DataFrame (output of clean.py)
        risk_free_rate: Risk-free rate as decimal

    Returns:
        DataFrame with implied_vol column added, rows where IV
        could not be computed are dropped
    """
    df = df.copy()

    df['T'] = df['days_to_expiry'] / 365.0

    df['implied_vol'] = df.apply(
        lambda row: compute_iv(
            market_price=row['mid_price'],
            S=row['underlying_price'],
            K=row['strike'],
            T=row['T'],
            r=risk_free_rate,
            option_type=row['option_type'],
        ),
        axis=1,
    )

    before = len(df)
    df = df.dropna(subset=['implied_vol'])
    after = len(df)

    print(f"IV computed for {after}/{before} options ({before - after} dropped)")

    df = df[(df['implied_vol'] >= 0.01) & (df['implied_vol'] <= 5.0)]

    return df.reset_index(drop=True)


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '.')
    from ingestion.fetch_options import fetch_options_chain
    from ingestion.fetch_macro import fetch_risk_free_rate
    from transform.clean import clean_options_data, add_moneyness, add_expiry_buckets

    ticker = "SPY"
    print(f"Fetching options for {ticker}...")
    raw = fetch_options_chain(ticker)

    print("Cleaning...")
    cleaned = clean_options_data(raw)
    cleaned = add_moneyness(cleaned)
    cleaned = add_expiry_buckets(cleaned)

    print("Fetching risk-free rate...")
    r = fetch_risk_free_rate()

    print("Computing IV surface...")
    surface = compute_iv_surface(cleaned, r)

    print(f"\nIV surface shape: {surface.shape}")
    print(surface[['ticker', 'strike', 'expiry_date', 'option_type',
                    'underlying_price', 'mid_price', 'implied_vol',
                    'moneyness_bucket', 'expiry_bucket']].head(10))

    print("\nIV stats:")
    print(surface['implied_vol'].describe())
