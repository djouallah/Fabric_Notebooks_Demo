
----------------- Setup Azure Secret and Attach DuckLake Data Warehouse --------------------------------------
-------------- fabric notebook provide a token automatically, for your laptop CLI is the easiest -------------



-------------------------------------------------------------
LOAD zipfs;
SET  force_download = true;


CREATE TABLE IF NOT EXISTS scada_today (
  DUID VARCHAR,
  INITIALMW DOUBLE,
  file VARCHAR,
  SETTLEMENTDATE TIMESTAMP,
  LASTCHANGED TIMESTAMP,
  DATE DATE,
  YEAR INT
);

CREATE TABLE IF NOT EXISTS price_today (
  REGIONID VARCHAR,
  RUNNO DOUBLE,
  DISPATCHINTERVAL DOUBLE,
  INTERVENTION DOUBLE,
  RRP DOUBLE,
  EEP DOUBLE,
  ROP DOUBLE,
  APCFLAG DOUBLE,
  MARKETSUSPENDEDFLAG DOUBLE,
  RAISE6SECRRP DOUBLE,
  RAISE6SECROP DOUBLE,
  RAISE6SECAPCFLAG DOUBLE,
  RAISE60SECRRP DOUBLE,
  RAISE60SECROP DOUBLE,
  RAISE60SECAPCFLAG DOUBLE,
  RAISE5MINRRP DOUBLE,
  RAISE5MINROP DOUBLE,
  RAISE5MINAPCFLAG DOUBLE,
  RAISEREGRRP DOUBLE,
  RAISEREGROP DOUBLE,
  RAISEREGAPCFLAG DOUBLE,
  LOWER6SECRRP DOUBLE,
  LOWER6SECROP DOUBLE,
  LOWER6SECAPCFLAG DOUBLE,
  LOWER60SECRRP DOUBLE,
  LOWER60SECROP DOUBLE,
  LOWER60SECAPCFLAG DOUBLE,
  LOWER5MINRRP DOUBLE,
  LOWER5MINROP DOUBLE,
  LOWER5MINAPCFLAG DOUBLE,
  LOWERREGRRP DOUBLE,
  LOWERREGROP DOUBLE,
  LOWERREGAPCFLAG DOUBLE,
  PRE_AP_ENERGY_PRICE DOUBLE,
  PRE_AP_RAISE6_PRICE DOUBLE,
  PRE_AP_RAISE60_PRICE DOUBLE,
  PRE_AP_RAISE5MIN_PRICE DOUBLE,
  PRE_AP_RAISEREG_PRICE DOUBLE,
  PRE_AP_LOWER6_PRICE DOUBLE,
  PRE_AP_LOWER60_PRICE DOUBLE,
  PRE_AP_LOWER5MIN_PRICE DOUBLE,
  PRE_AP_LOWERREG_PRICE DOUBLE,
  RAISE1SECRRP DOUBLE,
  RAISE1SECROP DOUBLE,
  RAISE1SECAPCFLAG DOUBLE,
  LOWER1SECRRP DOUBLE,
  LOWER1SECROP DOUBLE,
  LOWER1SECAPCFLAG DOUBLE,
  PRE_AP_RAISE1_PRICE DOUBLE,
  PRE_AP_LOWER1_PRICE DOUBLE,
  CUMUL_PRE_AP_ENERGY_PRICE DOUBLE,
  CUMUL_PRE_AP_RAISE6_PRICE DOUBLE,
  CUMUL_PRE_AP_RAISE60_PRICE DOUBLE,
  CUMUL_PRE_AP_RAISE5MIN_PRICE DOUBLE,
  CUMUL_PRE_AP_RAISEREG_PRICE DOUBLE,
  CUMUL_PRE_AP_LOWER6_PRICE DOUBLE,
  CUMUL_PRE_AP_LOWER60_PRICE DOUBLE,
  CUMUL_PRE_AP_LOWER5MIN_PRICE DOUBLE,
  CUMUL_PRE_AP_LOWERREG_PRICE DOUBLE,
  CUMUL_PRE_AP_RAISE1_PRICE DOUBLE,
  CUMUL_PRE_AP_LOWER1_PRICE DOUBLE,
  SETTLEMENTDATE TIMESTAMP,
  DATE DATE,
  file VARCHAR,
  YEAR INT
);

---------------------------------------------------- process scada and price data

create or replace temp table list_files_web as
WITH
  html_data AS (
    SELECT
      content AS html
    FROM
      read_text('http://nemweb.com.au/Reports/Current/Dispatch_SCADA/')
  ),
  lines AS (
    SELECT
      unnest(string_split(html, '<br>')) AS line
    FROM
      html_data
  )
SELECT
  'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url
FROM
  lines
WHERE
  line LIKE '%PUBLIC_DISPATCHSCADA%'
ORDER BY
  full_url DESC
LIMIT
  500;

SET VARIABLE list_of_files_scada = (
  SELECT
    list(full_url)
  FROM
    list_files_web
  WHERE
    split_part(regexp_extract(full_url, '/([^/]+\.zip)', 1), '.', 1) NOT IN (
      SELECT DISTINCT split_part(file, '.', 1) FROM scada_today
    )
);

-------------------------------------- staging area for scada data

create or replace temp view scada_staging as
  
    SELECT
      *
    FROM
      read_csv(
        list_transform(getvariable ('list_of_files_scada'), x -> 'zip://' || x || '/*.CSV'),
    Skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      'I' : 'VARCHAR',
      'DISPATCH' : 'VARCHAR',
      'UNIT_SCADA' : 'VARCHAR',
      'xx' : 'VARCHAR',
      'SETTLEMENTDATE' : 'timestamp',
      'DUID' : 'VARCHAR',
      'SCADAVALUE' : 'double',
      'LASTCHANGED' : 'timestamp'
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false
  )
  WHERE I = 'D'
    AND SCADAVALUE != 0 ;

INSERT INTO
  scada_today BY NAME
SELECT
  DUID,
  SCADAVALUE AS INITIALMW,
  SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS date) AS date,
  parse_filename(filename) AS file,
  year(SETTLEMENTDATE ) AS YEAR
FROM
  scada_staging ;
------------------------------------------------------------

-- Process price data
create or replace temp table list_files_web_price as
WITH
  html_data AS (
    SELECT
      content AS html
    FROM
      read_text('http://nemweb.com.au/Reports/Current/DispatchIS_Reports/')
  ),
  lines AS (
    SELECT
      unnest(string_split(html, '<br>')) AS line
    FROM
      html_data
  )
SELECT
  'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url
FROM
  lines
WHERE
  line LIKE '%PUBLIC_DISPATCHIS_%.zip%'
ORDER BY
  full_url DESC
LIMIT
  500;

SET VARIABLE list_of_files_price = (
  SELECT
    list(full_url)
  FROM
    list_files_web_price
  WHERE
    split_part(regexp_extract(full_url, '/([^/]+\.zip)', 1), '.', 1) NOT IN (
      SELECT DISTINCT split_part(file, '.', 1) FROM price_today
    )
);

-------------------------------------- staging area for price data

create or replace temp view price_staging as
  
WITH RAW_price AS (
    SELECT *
    FROM read_csv(
      list_transform(getvariable('list_of_files_price'), x -> 'zip://' || x || '/*.CSV'),
      Skip = 1,
      header = 0,
      all_varchar = 1,
      columns = {
        'I' : 'VARCHAR',
        'DISPATCH' : 'VARCHAR',
        'PRICE' : 'VARCHAR',
        'xx' : 'VARCHAR',
        'SETTLEMENTDATE' : 'timestamp',
        'RUNNO' : 'VARCHAR',
        'REGIONID' : 'VARCHAR',
        'DISPATCHINTERVAL' : 'VARCHAR',
        'INTERVENTION' : 'VARCHAR',
        'RRP' : 'VARCHAR',
        'EEP' : 'VARCHAR',
        'ROP' : 'VARCHAR',
        'APCFLAG' : 'VARCHAR',
        'MARKETSUSPENDEDFLAG' : 'VARCHAR',
        'LASTCHANGED' : 'VARCHAR',
        'RAISE6SECRRP' : 'VARCHAR',
        'RAISE6SECROP' : 'VARCHAR',
        'RAISE6SECAPCFLAG' : 'VARCHAR',
        'RAISE60SECRRP' : 'VARCHAR',
        'RAISE60SECROP' : 'VARCHAR',
        'RAISE60SECAPCFLAG' : 'VARCHAR',
        'RAISE5MINRRP' : 'VARCHAR',
        'RAISE5MINROP' : 'VARCHAR',
        'RAISE5MINAPCFLAG' : 'VARCHAR',
        'RAISEREGRRP' : 'VARCHAR',
        'RAISEREGROP' : 'VARCHAR',
        'RAISEREGAPCFLAG' : 'VARCHAR',
        'LOWER6SECRRP' : 'VARCHAR',
        'LOWER6SECROP' : 'VARCHAR',
        'LOWER6SECAPCFLAG' : 'VARCHAR',
        'LOWER60SECRRP' : 'VARCHAR',
        'LOWER60SECROP' : 'VARCHAR',
        'LOWER60SECAPCFLAG' : 'VARCHAR',
        'LOWER5MINRRP' : 'VARCHAR',
        'LOWER5MINROP' : 'VARCHAR',
        'LOWER5MINAPCFLAG' : 'VARCHAR',
        'LOWERREGRRP' : 'VARCHAR',
        'LOWERREGROP' : 'VARCHAR',
        'LOWERREGAPCFLAG' : 'VARCHAR',
        'PRICE_STATUS' : 'VARCHAR',
        'PRE_AP_ENERGY_PRICE' : 'VARCHAR',
        'PRE_AP_RAISE6_PRICE' : 'VARCHAR',
        'PRE_AP_RAISE60_PRICE' : 'VARCHAR',
        'PRE_AP_RAISE5MIN_PRICE' : 'VARCHAR',
        'PRE_AP_RAISEREG_PRICE' : 'VARCHAR',
        'PRE_AP_LOWER6_PRICE' : 'VARCHAR',
        'PRE_AP_LOWER60_PRICE' : 'VARCHAR',
        'PRE_AP_LOWER5MIN_PRICE' : 'VARCHAR',
        'PRE_AP_LOWERREG_PRICE' : 'VARCHAR',
        'RAISE1SECRRP' : 'VARCHAR',
        'RAISE1SECROP' : 'VARCHAR',
        'RAISE1SECAPCFLAG' : 'VARCHAR',
        'LOWER1SECRRP' : 'VARCHAR',
        'LOWER1SECROP' : 'VARCHAR',
        'LOWER1SECAPCFLAG' : 'VARCHAR',
        'PRE_AP_RAISE1_PRICE' : 'VARCHAR',
        'PRE_AP_LOWER1_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_ENERGY_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_RAISE6_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_RAISE60_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_RAISE5MIN_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_RAISEREG_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_LOWER6_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_LOWER60_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_LOWER5MIN_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_LOWERREG_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_RAISE1_PRICE' : 'VARCHAR',
        'CUMUL_PRE_AP_LOWER1_PRICE' : 'VARCHAR',
        'OCD_STATUS' : 'VARCHAR',
        'MII_STATUS' : 'VARCHAR',
      },
      filename = 1,
      null_padding = true,
      ignore_errors = 1,
      auto_detect = false
    )
    WHERE I = 'D'
      AND PRICE = 'PRICE'
  )
  SELECT
    REGIONID,
    CAST(columns(* EXCLUDE (SETTLEMENTDATE, REGIONID, I, xx, PRICE, filename, OCD_STATUS, MII_STATUS, DISPATCH, PRICE_STATUS, LASTCHANGED)) AS DOUBLE), -- Cast remaining columns to DOUBLE
    SETTLEMENTDATE,
    CAST(SETTLEMENTDATE AS date) AS date,
    parse_filename(filename) AS file,
    year(SETTLEMENTDATE) AS YEAR
  FROM RAW_price;

INSERT INTO
  price_today BY NAME
SELECT
  *
FROM
  price_staging;
------------------------------------------------------------
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

insert into summary BY NAME select * from summary_today_staging ;