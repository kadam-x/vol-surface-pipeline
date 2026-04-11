-- Cross-Ticker Vol Comparison
-- Compare 30-day ATM IV across multiple tickers
-- Usage: Relative value analysis, sector comparison

SELECT 
    f.snapshot_date AS date,
    dt.symbol AS ticker,
    AVG(f.implied_vol) AS atm_iv
FROM fact_options_snapshot f
JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
JOIN dim_strike ds ON f.strike_id = ds.strike_id
JOIN dim_expiry de ON f.expiry_id = de.expiry_id
WHERE ds.moneyness_bucket = 'ATM'
    AND f.implied_vol IS NOT NULL
    AND de.days_to_expiry BETWEEN 20 AND 40
    AND f.snapshot_date = CURRENT_DATE
GROUP BY f.snapshot_date, dt.symbol
ORDER BY atm_iv DESC;