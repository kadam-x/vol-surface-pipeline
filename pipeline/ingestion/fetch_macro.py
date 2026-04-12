import os
import httpx
from typing import Optional


def fetch_risk_free_rate(api_key: Optional[str] = None, use_fallback: bool = True) -> float:
    """
    Fetch US Treasury risk-free rate from FRED API.
    
    Args:
        api_key: FRED API key (optional)
        use_fallback: If True, return fallback rate when API unavailable
    
    Returns:
        Risk-free rate as decimal (e.g., 0.045 for 4.5%)
    """
    fallback_rate = float(os.getenv("RISK_FREE_RATE_FALLBACK", "0.045"))
    
    if not api_key:
        api_key = os.getenv("FRED_API_KEY")
    
    if not api_key and not use_fallback:
        raise ValueError("FRED API key required but not provided")
    
    if not api_key:
        print(f"No FRED API key found, using fallback rate: {fallback_rate}")
        return fallback_rate
    
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DGS10",
            "api_key": api_key,
            "sort_order": "desc",
            "limit": "1",
            "file_type": "json",
        }
        response = httpx.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        value = data['observations'][0]['value']
        if value == ".":
            raise ValueError("FRED returned missing value for DGS10")
        rate = float(value) / 100

        print(f"Fetched risk-free rate from FRED: {rate}")
        return rate
        
    except Exception as e:
        print(f"Failed to fetch from FRED: {e}, using fallback: {fallback_rate}")
        return fallback_rate


if __name__ == "__main__":
    rate = fetch_risk_free_rate()
    print(f"Risk-free rate: {rate}")
