-- Vol Smile
-- IV across strikes for a single expiry (shows smile/skew shape)
-- Usage: Visualize vol skew, identify risk preferences

SELECT 
    ds.strike_price,
    ds.moneyness_bucket,
    f.option_type,
    f.implied_vol,
    f.last_price AS option_price
FROM fact_options_snapshot f
JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
JOIN dim_strike ds ON f.strike_id = ds.strike_id
JOIN dim_expiry de ON f.expiry_id = de.expiry_id
WHERE dt.symbol = 'SPY'
    AND de.expiry_date = '2025-06-20'
    AND f.implied_vol IS NOT NULL
ORDER BY ds.strike_price;