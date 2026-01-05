
----------------- Summary Table Refresh --------------------------------------
-- This script refreshes the summary table by joining scada, price, and duid data
-- Run after scada/price processing to update summary with new data
------------------------------------------------------------------------------

-- Set flags to check if new data exists (variables set in scada.sql and scada_today.sql)
SET VARIABLE has_new_daily_scada = (SELECT COALESCE(getvariable('has_new_scada_daily'), false));

-------------------------------------- staging area for summary data (daily)

create or replace temp view summary_staging as
select
  s.DATE as date,
  cast(strftime(s.SETTLEMENTDATE, '%H%M') AS INT16) as time,
  (
    select
      max(cast(settlementdate as TIMESTAMP))
    from
      scada
  ) as cutoff,
  s.DUID,
  max(s.INITIALMW) as mw,
  max(p.RRP) as price
from
  scada s
  LEFT JOIN duid d ON s.DUID = d.DUID
  LEFT JOIN price p ON s.SETTLEMENTDATE = p.SETTLEMENTDATE
  AND d.Region = p.REGIONID
where
  s.INTERVENTION = 0
  and INITIALMW <> 0
  and p.INTERVENTION = 0
group by all
ORDER BY
  date,
  s.DUID,
  time,
  price;

-------------------------------------- refresh summary table (daily)

-- Refresh summary if new daily scada data OR summary is empty (first run)
SET VARIABLE needs_summary_refresh = (SELECT getvariable('has_new_daily_scada') OR NOT EXISTS (SELECT 1 FROM summary));

SELECT '[DAILY SUMMARY] Rebuilding summary table...' AS Status WHERE getvariable('needs_summary_refresh');
SELECT '[DAILY SUMMARY] Skipped - no new daily scada data' AS Status WHERE NOT getvariable('needs_summary_refresh');

DELETE FROM summary WHERE getvariable('needs_summary_refresh');
INSERT INTO summary BY NAME SELECT * FROM summary_staging WHERE getvariable('needs_summary_refresh');

SELECT '[DAILY SUMMARY] Loaded ' || count(*) || ' summary records' AS Status FROM summary WHERE getvariable('needs_summary_refresh');

-------------------------------------- staging area for summary data (intraday)

SET VARIABLE max_timestamp = (SELECT max(cutoff) from summary );

create or replace temp view summary_today_staging as
with incremental AS (
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
        INITIALMW <> 0
        AND p.INTERVENTION = 0
        AND s.date           >= cast(getvariable('max_timestamp') as date)
        AND s.settlementdate > getvariable('max_timestamp')
        AND p.settlementdate > getvariable('max_timestamp')
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
ORDER BY date, DUID, time;

-------------------------------------- refresh summary table (intraday)

SET VARIABLE intraday_count_before = (SELECT count(*) FROM summary);

INSERT INTO summary BY NAME SELECT * FROM summary_today_staging;

SET VARIABLE intraday_count_after = (SELECT count(*) FROM summary);
SET VARIABLE intraday_added = (SELECT getvariable('intraday_count_after') - getvariable('intraday_count_before'));

SELECT '[INTRADAY SUMMARY] Added ' || getvariable('intraday_added') || ' intraday records to summary' AS Status WHERE getvariable('intraday_added') > 0;
SELECT '[INTRADAY SUMMARY] Skipped - no new intraday data' AS Status WHERE getvariable('intraday_added') = 0;
------------------------------------------------------------
