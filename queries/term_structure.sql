-- Lejárat Szerkezet (Term Structure)
-- IV lejárat bucket szerint egy adott napon egy adott tickerre
-- Felhasználás: Forward vol várakozások elemzése, term structure dinamika

SELECT 
    de.expiry_bucket,
    de.expiry_date,
    de.days_to_expiry,
    AVG(f.implied_vol) AS avg_iv,
    COUNT(*) AS num_options
FROM fact_options_snapshot f
JOIN dim_ticker dt ON f.ticker_id = dt.ticker_id
JOIN dim_strike ds ON f.strike_id = ds.strike_id
JOIN dim_expiry de ON f.expiry_id = de.expiry_id
WHERE dt.symbol = 'SPY'
    AND f.snapshot_date = CURRENT_DATE
    AND f.implied_vol IS NOT NULL
GROUP BY de.expiry_bucket, de.expiry_date, de.days_to_expiry
ORDER BY de.days_to_expiry;