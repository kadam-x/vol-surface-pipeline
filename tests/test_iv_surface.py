import pytest
import numpy as np
import pandas as pd
from pipeline.transform.iv_surface import (
    black_scholes_price,
    implied_volatility,
    compute_iv_surface,
)


class TestBlackScholes:
    def test_call_price_atm(self):
        price = black_scholes_price(S=100, K=100, T=1, r=0.05, sigma=0.2, option_type='call')
        assert 7 < price < 12
    
    def test_call_price_itm(self):
        price = black_scholes_price(S=100, K=80, T=1, r=0.05, sigma=0.2, option_type='call')
        assert price > 15
    
    def test_call_price_otm(self):
        price = black_scholes_price(S=100, K=120, T=1, r=0.05, sigma=0.2, option_type='call')
        assert 0 < price < 5
    
    def test_put_price_atm(self):
        price = black_scholes_price(S=100, K=100, T=1, r=0.05, sigma=0.2, option_type='put')
        assert 7 < price < 12
    
    def test_zero_time(self):
        price = black_scholes_price(S=100, K=100, T=0, r=0.05, sigma=0.2, option_type='call')
        assert price == 0.0
    
    def test_zero_vol(self):
        price = black_scholes_price(S=100, K=100, T=1, r=0.05, sigma=0, option_type='call')
        assert price == 0.0


class TestImpliedVolatility:
    def test_iv_atm(self):
        S, K, T, r = 100, 100, 1, 0.05
        market_price = black_scholes_price(S, K, T, r, 0.2, 'call')
        iv = implied_volatility(S, K, T, r, market_price, 'call')
        assert iv is not None
        assert abs(iv - 0.2) < 0.01
    
    def test_iv_otm_call(self):
        S, K, T, r = 100, 110, 1, 0.05
        market_price = black_scholes_price(S, K, T, r, 0.25, 'call')
        iv = implied_volatility(S, K, T, r, market_price, 'call')
        assert iv is not None
        assert abs(iv - 0.25) < 0.01
    
    def test_iv_put(self):
        S, K, T, r = 100, 100, 1, 0.05
        market_price = black_scholes_price(S, K, T, r, 0.2, 'put')
        iv = implied_volatility(S, K, T, r, market_price, 'put')
        assert iv is not None
        assert abs(iv - 0.2) < 0.01
    
    def test_iv_zero_price(self):
        iv = implied_volatility(S=100, K=100, T=1, r=0.05, market_price=0, option_type='call')
        assert iv is None
    
    def test_iv_negative_time(self):
        iv = implied_volatility(S=100, K=100, T=-1, r=0.05, market_price=10, option_type='call')
        assert iv is None


class TestComputeIVSurface:
    def test_compute_surface(self):
        df = pd.DataFrame({
            'strike': [100, 105, 110],
            'expiry_date': pd.to_datetime(['2025-06-20', '2025-06-20', '2025-06-20']),
            'last_price': [5.0, 2.5, 1.0],
            'option_type': ['call', 'call', 'call'],
        })
        
        result = compute_iv_surface(df, spot_price=100, risk_free_rate=0.05)
        
        assert 'implied_vol' in result.columns
        assert result['implied_vol'].notna().any()
    
    def test_filter_expired(self):
        df = pd.DataFrame({
            'strike': [100],
            'expiry_date': pd.to_datetime(['2020-01-01']),
            'last_price': [5.0],
            'option_type': ['call'],
        })
        
        result = compute_iv_surface(df, spot_price=100, risk_free_rate=0.05)
        assert len(result) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])