CREATE VIEW if not exists summary(cutoff) AS SELECT '1900-01-01';
SET VARIABLE max_timestamp = (SELECT max(cutoff) from summary );
WITH reload AS (
    SELECT
        s.date,
        s.SETTLEMENTDATE,
        s.DUID,
        max(s.INITIALMW) AS mw,
        max(p.RRP) AS price
    FROM scada s
    LEFT JOIN duid d ON s.DUID = d.DUID
    LEFT JOIN price p ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
    WHERE
        s.INTERVENTION = 0
        AND INITIALMW <> 0
        AND p.INTERVENTION = 0
        AND s.settlementdate > getvariable('max_timestamp')
    GROUP BY ALL
),
incremental AS (
    SELECT
        s.date,
        s.SETTLEMENTDATE,
        s.DUID,
        max(s.INITIALMW)  AS mw,
        max(p.RRP)  AS price
    FROM scada_today s
    JOIN duid d ON s.DUID = d.DUID
    JOIN (SELECT * FROM price_today WHERE INTERVENTION = 0  and date  >= cast(getvariable('max_timestamp') as date)) p
        ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
    WHERE
        s.INTERVENTION = 0
        AND INITIALMW <> 0
        AND p.INTERVENTION = 0
        AND s.date           >= cast(getvariable('max_timestamp') as date)
        AND s.settlementdate > getvariable('max_timestamp')
        AND p.settlementdate > getvariable('max_timestamp')
    GROUP BY ALL
),
combined AS (
    SELECT * FROM reload
    UNION ALL 
    SELECT * FROM incremental
),
deduplicated_max_by AS (
    SELECT
        date,
        SETTLEMENTDATE,
        DUID,
        MAX(mw) AS mw,                                  
        MAX(price) AS price                              
    FROM combined
    GROUP BY all
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
    FROM deduplicated_max_by
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
