import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
from typing import Optional
from datetime import datetime


def black_scholes_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str,
) -> float:
    """
    Calculate Black-Scholes option price.
    
    Args:
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        sigma: Volatility
        option_type: 'call' or 'put'
    
    Returns:
        Option price
    """
    if T <= 0 or sigma <= 0:
        return 0.0
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    if option_type.lower() == 'call':
        price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    
    return price


def implied_volatility(
    S: float,
    K: float,
    T: float,
    r: float,
    market_price: float,
    option_type: str,
    tol: float = 1e-6,
) -> Optional[float]:
    """
    Compute implied volatility using Black-Scholes inversion.
    
    Uses Brent's method to solve for sigma that produces the market price.
    
    Args:
        S: Spot price
        K: Strike price
        T: Time to expiry (years)
        r: Risk-free rate
        market_price: Observed option price
        option_type: 'call' or 'put'
        tol: Tolerance for convergence
    
    Returns:
        Implied volatility as decimal (e.g., 0.20 for 20%), or None if failed
    """
    if T <= 0 or market_price <= 0:
        return None
    
    intrinsic = max(0, S - K) if option_type.lower() == 'call' else max(0, K - S)
    if market_price <= intrinsic:
        return None
    
    def objective(sigma):
        return black_scholes_price(S, K, T, r, sigma, option_type) - market_price
    
    try:
        iv = brentq(objective, 0.001, 5.0, xtol=tol)
        return float(iv)
    except (ValueError, RuntimeError):
        return None


def compute_iv_surface(
    df: pd.DataFrame,
    spot_price: float,
    risk_free_rate: float,
    reference_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """
    Compute implied volatility for all options in the DataFrame.
    
    Args:
        df: Options DataFrame (cleaned, with strike, expiry_date, last_price, option_type)
        spot_price: Current underlying price
        risk_free_rate: Risk-free rate as decimal
        reference_date: Reference date for time calculation
    
    Returns:
        DataFrame with added 'implied_vol' column
    """
    df = df.copy()
    
    if reference_date is None:
        reference_date = pd.Timestamp.now().normalize()
    
    df['T'] = (df['expiry_date'] - reference_date).dt.days / 365.0
    df = df[df['T'] > 0]
    
    iv_values = []
    for _, row in df.iterrows():
        iv = implied_volatility(
            S=spot_price,
            K=row['strike'],
            T=row['T'],
            r=risk_free_rate,
            market_price=row['last_price'],
            option_type=row['option_type'],
        )
        iv_values.append(iv)
    
    df['implied_vol'] = iv_values
    
    return df.reset_index(drop=True)


if __name__ == "__main__":
    from ingestion.fetch_options import fetch_underlying_price
    
    spot = fetch_underlying_price("SPY")
    print(f"SPY spot: {spot}")
    
    result = compute_iv_surface(
        pd.DataFrame({
            'strike': [500, 510, 520],
            'expiry_date': pd.to_datetime(['2025-06-20', '2025-06-20', '2025-06-20']),
            'last_price': [5.0, 2.5, 1.0],
            'option_type': ['call', 'call', 'call'],
        }),
        spot_price=spot,
        risk_free_rate=0.045,
    )
    print(result)