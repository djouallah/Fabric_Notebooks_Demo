CREATE VIEW IF NOT EXISTS summary(date) AS SELECT '1900-01-01';

WITH date_bounds AS (
    -- Get the earliest date in summary (excluding dummy date)
    SELECT MIN(date) AS min_date
    FROM summary
    WHERE date > '1900-01-01'
),
available_dates AS (
    -- Get all missing dates before min_date
    SELECT DISTINCT s.date
    FROM scada s
    CROSS JOIN date_bounds db
    WHERE s.date NOT IN (SELECT DISTINCT date FROM summary)
      AND s.INTERVENTION = 0
      AND s.INITIALMW <> 0
      AND s.date < db.min_date
    ORDER BY s.date DESC
),
contiguous_batch AS (
    -- Find the longest contiguous sequence working backwards from min_date
    SELECT 
        ad.date,
        ROW_NUMBER() OVER (ORDER BY ad.date DESC) as rn,
        ad.date + CAST(ROW_NUMBER() OVER (ORDER BY ad.date DESC) AS INTEGER) as date_plus_rn
    FROM available_dates ad
    CROSS JOIN date_bounds db
    WHERE ad.date <= db.min_date - 1
),
contiguous_dates AS (
    -- Group by the date_plus_rn to find contiguous sequences
    -- The longest contiguous sequence will have the same date_plus_rn value
    SELECT date
    FROM contiguous_batch
    WHERE date_plus_rn = (
        SELECT date_plus_rn
        FROM contiguous_batch
        GROUP BY date_plus_rn
        ORDER BY COUNT(*) DESC, date_plus_rn DESC
        LIMIT 1
    )
),
incremental AS (
    SELECT
        s.date,
        s.SETTLEMENTDATE,
        s.DUID,
        MAX(s.INITIALMW) AS mw,
        MAX(p.RRP) AS price
    FROM scada s
    JOIN duid d ON s.DUID = d.DUID
    JOIN (
        SELECT * 
        FROM price 
        WHERE INTERVENTION = 0 
          AND date IN (SELECT date FROM contiguous_dates)
    ) p ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
    WHERE s.INTERVENTION = 0
      AND s.INITIALMW <> 0
      AND s.date IN (SELECT date FROM contiguous_dates)
    GROUP BY ALL
),
final_with_cutoff AS (
    SELECT
        date,
        SETTLEMENTDATE,
        DUID,
        CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT16) AS time,
        CAST(mw AS DECIMAL(18, 4)) AS mw,
        CAST(price AS DECIMAL(18, 4)) AS price,
        MAX(SETTLEMENTDATE) OVER () AS cutoff
    FROM incremental
)
SELECT
    date,
    time,
    DUID,
    mw,
    price,
    cutoff
FROM final_with_cutoff
ORDER BY date, DUID, time, price;
