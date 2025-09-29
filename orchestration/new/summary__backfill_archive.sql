CREATE VIEW IF NOT EXISTS summary(date) AS SELECT '1900-01-01';

WITH date_bounds AS (
    SELECT MIN(date) AS min_date
    FROM summary
    WHERE date > '1900-01-01'
),
available_dates AS (
    SELECT DISTINCT s.date
    FROM scada s
    CROSS JOIN date_bounds db
    WHERE s.date NOT IN (SELECT DISTINCT date FROM summary)
      AND s.INTERVENTION = 0
      AND s.INITIALMW <> 0
      AND s.date < db.min_date
),
numbered_dates AS (
    -- Number dates from min_date backwards
    SELECT 
        ad.date,
        (SELECT min_date FROM date_bounds) - ad.date AS days_before_min,
        ROW_NUMBER() OVER (ORDER BY ad.date DESC) AS rn
    FROM available_dates ad
    ORDER BY ad.date DESC
),
contiguous_dates AS (
    -- Only take dates where days_before_min equals row number
    -- This ensures: min_date-1, min_date-2, min_date-3, etc (no gaps)
    SELECT date
    FROM numbered_dates
    WHERE days_before_min = rn
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
