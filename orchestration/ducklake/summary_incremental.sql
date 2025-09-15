CREATE TABLE IF NOT EXISTS summary(date DATE,"time" SMALLINT, cutoff TIMESTAMP WITH TIME ZONE, DUID VARCHAR,  mw DECIMAL(18,4), price DECIMAL(18,4));

ALTER TABLE if exists summary     SET PARTITIONED BY (year(date));

SET VARIABLE max_timestamp = (SELECT max(cutoff) from summary );

insert into summary BY NAME
with incremental as (
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
    GROUP BY ALL)
    SELECT
    date,
    CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT16) AS time,
    DUID,
    mw,
    price,
    MAX(SETTLEMENTDATE) OVER () AS cutoff
FROM incremental
ORDER BY date, DUID, time, price;