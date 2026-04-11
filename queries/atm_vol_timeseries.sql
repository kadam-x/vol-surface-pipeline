-- ATM Vol Time Series
-- Track at-the-money IV per ticker over time
-- Usage: Monitor IV trends, detect vol regime changes

SELECT 
    f.snapshot_date AS date,
    dt.symbol AS ticker,
    AVG(f.implied_vol) AS avg_iv
FROM fact_options_snapshot f
JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
JOIN dim_strike ds ON f.strike_id = ds.strike_id
JOIN dim_expiry de ON f.expiry_id = de.expiry_id
WHERE ds.moneyness_bucket = 'ATM'
    AND f.implied_vol IS NOT NULL
    AND f.snapshot_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY f.snapshot_date, dt.symbol
ORDER BY f.snapshot_date;