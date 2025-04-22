-- materialized: (summary,delta,append)
SET VARIABLE max_timestamp =
    (SELECT max(cutoff)
     FROM delta_scan('abfss://processing@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Tables/aemo/summary'));
CREATE or replace view duid as select * from delta_scan('abfss://processing@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Tables/aemo/duid');

SELECT
    s.date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT16) AS time,
    (SELECT max(CAST(settlementdate AS TIMESTAMPTZ))
     FROM scada_today
     WHERE date >= CAST(getvariable('max_timestamp') AS DATE)) AS cutoff,
    s.DUID,
    CAST(max(s.INITIALMW) AS DECIMAL(18, 4)) AS mw,
    CAST(max(p.RRP) AS DECIMAL(18, 4)) AS price
FROM
    scada_today s
JOIN
    duid d ON s.DUID = d.DUID
JOIN
    (SELECT *
     FROM price_today
     WHERE INTERVENTION = 0
       AND date >= CAST(getvariable('max_timestamp') AS DATE)) p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE
    AND d.Region = p.REGIONID
WHERE
    s.INTERVENTION = 0
    AND INITIALMW <> 0
    AND s.settlementdate > getvariable('max_timestamp')
    AND p.settlementdate > getvariable('max_timestamp')
    AND s.date >= CAST(getvariable('max_timestamp') AS DATE)
GROUP BY
    ALL
ORDER BY
    s.date, s.DUID, time, price;