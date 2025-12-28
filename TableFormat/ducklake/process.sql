
----------------- Setup Azure Secret and Attach DuckLake Data Warehouse --------------------------------------
-------------- fabric notebook provide a token automatically, for your laptop CLI is the easiest -------------

CREATE or replace   SECRET secret_azure (
    TYPE azure,
    PROVIDER credential_chain,
    CHAIN 'cli',
    ACCOUNT_NAME 'onelake'
);

SET VARIABLE row_limit = 60;
SET VARIABLE DATA_PATH = 'abfss://ducklake@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Tables';

----------------------------------------------------------------------------------------------------------------

SELECT '[CONFIG] Processing ' || getvariable('row_limit') || ' files per batch' AS Status;

ATTACH or replace 'ducklake:sqlite:bronze.db' AS dwh (DATA_PATH getvariable('DATA_PATH')) ;
  USE dwh ;
  CALL set_option('parquet_row_group_size', 2048*1000) ;
  CALL set_option('target_file_size', '128MB') ;
  CALL set_option('parquet_compression', 'ZSTD');
  CALL set_option('parquet_version', 1);
  CALL set_option('rewrite_delete_threshold', 0);

create schema  if not exists dwh.bronze;
use dwh.bronze;


-------------------------------------------------------------

LOAD zipfs;
SET  force_download = true;

--- define tables

CREATE TABLE IF NOT EXISTS scada (
  UNIT VARCHAR,
  DUID VARCHAR,
  VERSION DOUBLE,
  RUNNO DOUBLE,
  INTERVENTION DOUBLE,
  DISPATCHMODE DOUBLE,
  AGCSTATUS DOUBLE,
  INITIALMW DOUBLE,
  TOTALCLEARED DOUBLE,
  RAMPDOWNRATE DOUBLE,
  RAMPUPRATE DOUBLE,
  LOWER5MIN DOUBLE,
  LOWER60SEC DOUBLE,
  LOWER6SEC DOUBLE,
  RAISE5MIN DOUBLE,
  RAISE60SEC DOUBLE,
  RAISE6SEC DOUBLE,
  MARGINAL5MINVALUE DOUBLE,
  MARGINAL60SECVALUE DOUBLE,
  MARGINAL6SECVALUE DOUBLE,
  MARGINALVALUE DOUBLE,
  VIOLATION5MINDEGREE DOUBLE,
  VIOLATION60SECDEGREE DOUBLE,
  VIOLATION6SECDEGREE DOUBLE,
  VIOLATIONDEGREE DOUBLE,
  LOWERREG DOUBLE,
  RAISEREG DOUBLE,
  AVAILABILITY DOUBLE,
  RAISE6SECFLAGS DOUBLE,
  RAISE60SECFLAGS DOUBLE,
  RAISE5MINFLAGS DOUBLE,
  RAISEREGFLAGS DOUBLE,
  LOWER6SECFLAGS DOUBLE,
  LOWER60SECFLAGS DOUBLE,
  LOWER5MINFLAGS DOUBLE,
  LOWERREGFLAGS DOUBLE,
  RAISEREGAVAILABILITY DOUBLE,
  RAISEREGENABLEMENTMAX DOUBLE,
  RAISEREGENABLEMENTMIN DOUBLE,
  LOWERREGAVAILABILITY DOUBLE,
  LOWERREGENABLEMENTMAX DOUBLE,
  LOWERREGENABLEMENTMIN DOUBLE,
  RAISE6SECACTUALAVAILABILITY DOUBLE,
  RAISE60SECACTUALAVAILABILITY DOUBLE,
  RAISE5MINACTUALAVAILABILITY DOUBLE,
  RAISEREGACTUALAVAILABILITY DOUBLE,
  LOWER6SECACTUALAVAILABILITY DOUBLE,
  LOWER60SECACTUALAVAILABILITY DOUBLE,
  LOWER5MINACTUALAVAILABILITY DOUBLE,
  LOWERREGACTUALAVAILABILITY DOUBLE,
  file VARCHAR,
  SETTLEMENTDATE TIMESTAMP,
  DATE DATE,
  YEAR INT
);


CREATE TABLE IF NOT EXISTS price (
  UNIT VARCHAR,
  REGIONID VARCHAR,
  VERSION DOUBLE,
  RUNNO DOUBLE,
  INTERVENTION DOUBLE,
  RRP DOUBLE,
  EEP DOUBLE,
  ROP DOUBLE,
  APCFLAG DOUBLE,
  MARKETSUSPENDEDFLAG DOUBLE,
  TOTALDEMAND DOUBLE,
  DEMANDFORECAST DOUBLE,
  DISPATCHABLEGENERATION DOUBLE,
  DISPATCHABLELOAD DOUBLE,
  NETINTERCHANGE DOUBLE,
  EXCESSGENERATION DOUBLE,
  LOWER5MINDISPATCH DOUBLE,
  LOWER5MINIMPORT DOUBLE,
  LOWER5MINLOCALDISPATCH DOUBLE,
  LOWER5MINLOCALPRICE DOUBLE,
  LOWER5MINLOCALREQ DOUBLE,
  LOWER5MINPRICE DOUBLE,
  LOWER5MINREQ DOUBLE,
  LOWER5MINSUPPLYPRICE DOUBLE,
  LOWER60SECDISPATCH DOUBLE,
  LOWER60SECIMPORT DOUBLE,
  LOWER60SECLOCALDISPATCH DOUBLE,
  LOWER60SECLOCALPRICE DOUBLE,
  LOWER60SECLOCALREQ DOUBLE,
  LOWER60SECPRICE DOUBLE,
  LOWER60SECREQ DOUBLE,
  LOWER60SECSUPPLYPRICE DOUBLE,
  LOWER6SECDISPATCH DOUBLE,
  LOWER6SECIMPORT DOUBLE,
  LOWER6SECLOCALDISPATCH DOUBLE,
  LOWER6SECLOCALPRICE DOUBLE,
  LOWER6SECLOCALREQ DOUBLE,
  LOWER6SECPRICE DOUBLE,
  LOWER6SECREQ DOUBLE,
  LOWER6SECSUPPLYPRICE DOUBLE,
  RAISE5MINDISPATCH DOUBLE,
  RAISE5MINIMPORT DOUBLE,
  RAISE5MINLOCALDISPATCH DOUBLE,
  RAISE5MINLOCALPRICE DOUBLE,
  RAISE5MINLOCALREQ DOUBLE,
  RAISE5MINPRICE DOUBLE,
  RAISE5MINREQ DOUBLE,
  RAISE5MINSUPPLYPRICE DOUBLE,
  RAISE60SECDISPATCH DOUBLE,
  RAISE60SECIMPORT DOUBLE,
  RAISE60SECLOCALDISPATCH DOUBLE,
  RAISE60SECLOCALPRICE DOUBLE,
  RAISE60SECLOCALREQ DOUBLE,
  RAISE60SECPRICE DOUBLE,
  RAISE60SECREQ DOUBLE,
  RAISE60SECSUPPLYPRICE DOUBLE,
  RAISE6SECDISPATCH DOUBLE,
  RAISE6SECIMPORT DOUBLE,
  RAISE6SECLOCALDISPATCH DOUBLE,
  RAISE6SECLOCALPRICE DOUBLE,
  RAISE6SECLOCALREQ DOUBLE,
  RAISE6SECPRICE DOUBLE,
  RAISE6SECREQ DOUBLE,
  RAISE6SECSUPPLYPRICE DOUBLE,
  AGGREGATEDISPATCHERROR DOUBLE,
  AVAILABLEGENERATION DOUBLE,
  AVAILABLELOAD DOUBLE,
  INITIALSUPPLY DOUBLE,
  CLEAREDSUPPLY DOUBLE,
  LOWERREGIMPORT DOUBLE,
  LOWERREGLOCALDISPATCH DOUBLE,
  LOWERREGLOCALREQ DOUBLE,
  LOWERREGREQ DOUBLE,
  RAISEREGIMPORT DOUBLE,
  RAISEREGLOCALDISPATCH DOUBLE,
  RAISEREGLOCALREQ DOUBLE,
  RAISEREGREQ DOUBLE,
  RAISE5MINLOCALVIOLATION DOUBLE,
  RAISEREGLOCALVIOLATION DOUBLE,
  RAISE60SECLOCALVIOLATION DOUBLE,
  RAISE6SECLOCALVIOLATION DOUBLE,
  LOWER5MINLOCALVIOLATION DOUBLE,
  LOWERREGLOCALVIOLATION DOUBLE,
  LOWER60SECLOCALVIOLATION DOUBLE,
  LOWER6SECLOCALVIOLATION DOUBLE,
  RAISE5MINVIOLATION DOUBLE,
  RAISEREGVIOLATION DOUBLE,
  RAISE60SECVIOLATION DOUBLE,
  RAISE6SECVIOLATION DOUBLE,
  LOWER5MINVIOLATION DOUBLE,
  LOWERREGVIOLATION DOUBLE,
  LOWER60SECVIOLATION DOUBLE,
  LOWER6SECVIOLATION DOUBLE,
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
  RAISE6SECACTUALAVAILABILITY DOUBLE,
  RAISE60SECACTUALAVAILABILITY DOUBLE,
  RAISE5MINACTUALAVAILABILITY DOUBLE,
  RAISEREGACTUALAVAILABILITY DOUBLE,
  LOWER6SECACTUALAVAILABILITY DOUBLE,
  LOWER60SECACTUALAVAILABILITY DOUBLE,
  LOWER5MINACTUALAVAILABILITY DOUBLE,
  LOWERREGACTUALAVAILABILITY DOUBLE,
  LORSURPLUS DOUBLE,
  LRCSURPLUS DOUBLE,
  file VARCHAR,
  SETTLEMENTDATE TIMESTAMP,
  DATE DATE,
  YEAR INT
);

CREATE TABLE IF NOT EXISTS summary(
  date DATE,
  "time" INT,
  cutoff TIMESTAMP,
  DUID VARCHAR,
  mw DECIMAL(18, 4),
  price DECIMAL(18, 4)
);

CREATE TABLE IF NOT EXISTS calendar(
  date DATE,
  year INT,
  month INT
);

CREATE TABLE IF NOT EXISTS duid(
  DUID VARCHAR,
  Region VARCHAR,
  FuelSourceDescriptor VARCHAR,
  Participant VARCHAR,
  State VARCHAR,
  latitude DOUBLE,
  longitude DOUBLE
);

------------------------------------------------------------------------------

INSERT INTO calendar
SELECT cast(unnest(generate_series(cast ('2018-04-01' as date), cast('2026-12-31' as date), interval 1 day)) as date) as date,
EXTRACT(year from date) as year,
EXTRACT(month from date) as month
WHERE NOT EXISTS (SELECT 1 FROM calendar);


---------------------------------------------------- process scada and price data

create or replace temp table list_files_web as
WITH
  html_data AS (
    SELECT
      content AS html
    FROM
      read_text('https://nemweb.com.au/Reports/Current/Daily_Reports')
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
  line LIKE '%PUBLIC_DAILY%'
ORDER BY
  full_url DESC
;

SELECT '[FILES] Found ' || count(*) || ' available files from NEMWEB' AS Status FROM list_files_web;

SET VARIABLE list_of_files_scada = (
  SELECT
    list(full_url)
  FROM (
    SELECT full_url
    FROM list_files_web
    WHERE
      split_part(regexp_extract(full_url, '/([^/]+\.zip)', 1), '.', 1) NOT IN (
        SELECT DISTINCT split_part(file, '.', 1) FROM scada
      )
    LIMIT getvariable('row_limit')
  )
);

SELECT '[SCADA] Processing ' || len(getvariable('list_of_files_scada')) || ' files' AS Status;
SELECT unnest(getvariable('list_of_files_scada')) AS scada_files;

SET VARIABLE list_of_files_price = (
  SELECT
    list(full_url)
  FROM (
    SELECT full_url
    FROM list_files_web
    WHERE
      split_part(regexp_extract(full_url, '/([^/]+\.zip)', 1), '.', 1) NOT IN (
        SELECT DISTINCT split_part(file, '.', 1) FROM price
      )
    LIMIT getvariable('row_limit')
  )
);

SELECT '[PRICE] Processing ' || len(getvariable('list_of_files_price')) || ' files' AS Status;
SELECT unnest(getvariable('list_of_files_price')) AS price_files;

-------------------------------------- staging area for scada and price data

create or replace temp view scada_staging as
  
    SELECT
      *
    FROM
      read_csv(
        list_transform(getvariable ('list_of_files_scada'), x -> 'zip://' || x || '/*.CSV'),
        skip = 1,
        header = 0,
        all_varchar = 1,
        columns = {
          'I': 'VARCHAR',
          'UNIT': 'VARCHAR',
          'XX': 'VARCHAR',
          'VERSION': 'VARCHAR',
          'SETTLEMENTDATE': 'VARCHAR',
          'RUNNO': 'VARCHAR',
          'DUID': 'VARCHAR',
          'INTERVENTION': 'VARCHAR',
          'DISPATCHMODE': 'VARCHAR',
          'AGCSTATUS': 'VARCHAR',
          'INITIALMW': 'VARCHAR',
          'TOTALCLEARED': 'VARCHAR',
          'RAMPDOWNRATE': 'VARCHAR',
          'RAMPUPRATE': 'VARCHAR',
          'LOWER5MIN': 'VARCHAR',
          'LOWER60SEC': 'VARCHAR',
          'LOWER6SEC': 'VARCHAR',
          'RAISE5MIN': 'VARCHAR',
          'RAISE60SEC': 'VARCHAR',
          'RAISE6SEC': 'VARCHAR',
          'MARGINAL5MINVALUE': 'VARCHAR',
          'MARGINAL60SECVALUE': 'VARCHAR',
          'MARGINAL6SECVALUE': 'VARCHAR',
          'MARGINALVALUE': 'VARCHAR',
          'VIOLATION5MINDEGREE': 'VARCHAR',
          'VIOLATION60SECDEGREE': 'VARCHAR',
          'VIOLATION6SECDEGREE': 'VARCHAR',
          'VIOLATIONDEGREE': 'VARCHAR',
          'LOWERREG': 'VARCHAR',
          'RAISEREG': 'VARCHAR',
          'AVAILABILITY': 'VARCHAR',
          'RAISE6SECFLAGS': 'VARCHAR',
          'RAISE60SECFLAGS': 'VARCHAR',
          'RAISE5MINFLAGS': 'VARCHAR',
          'RAISEREGFLAGS': 'VARCHAR',
          'LOWER6SECFLAGS': 'VARCHAR',
          'LOWER60SECFLAGS': 'VARCHAR',
          'LOWER5MINFLAGS': 'VARCHAR',
          'LOWERREGFLAGS': 'VARCHAR',
          'RAISEREGAVAILABILITY': 'VARCHAR',
          'RAISEREGENABLEMENTMAX': 'VARCHAR',
          'RAISEREGENABLEMENTMIN': 'VARCHAR',
          'LOWERREGAVAILABILITY': 'VARCHAR',
          'LOWERREGENABLEMENTMAX': 'VARCHAR',
          'LOWERREGENABLEMENTMIN': 'VARCHAR',
          'RAISE6SECACTUALAVAILABILITY': 'VARCHAR',
          'RAISE60SECACTUALAVAILABILITY': 'VARCHAR',
          'RAISE5MINACTUALAVAILABILITY': 'VARCHAR',
          'RAISEREGACTUALAVAILABILITY': 'VARCHAR',
          'LOWER6SECACTUALAVAILABILITY': 'VARCHAR',
          'LOWER60SECACTUALAVAILABILITY': 'VARCHAR',
          'LOWER5MINACTUALAVAILABILITY': 'VARCHAR',
          'LOWERREGACTUALAVAILABILITY': 'VARCHAR'
        },
        filename = 1,
        null_padding = true,
        ignore_errors = 1,
        auto_detect = false
      )
    WHERE
      I = 'D'
      AND UNIT = 'DUNIT'
      AND VERSION = '3'
  ;


create or replace temp view price_staging as
  
    SELECT
      *
    FROM
      read_csv(
        list_transform(getvariable ('list_of_files_price'), x -> 'zip://' || x || '/*.CSV'),
        skip = 1,
        header = 0,
        all_varchar = 1,
        columns = {
          'I': 'VARCHAR',
          'UNIT': 'VARCHAR',
          'XX': 'VARCHAR',
          'VERSION': 'VARCHAR',
          'SETTLEMENTDATE': 'VARCHAR',
          'RUNNO': 'VARCHAR',
          'REGIONID': 'VARCHAR',
          'INTERVENTION': 'VARCHAR',
          'RRP': 'VARCHAR',
          'EEP': 'VARCHAR',
          'ROP': 'VARCHAR',
          'APCFLAG': 'VARCHAR',
          'MARKETSUSPENDEDFLAG': 'VARCHAR',
          'TOTALDEMAND': 'VARCHAR',
          'DEMANDFORECAST': 'VARCHAR',
          'DISPATCHABLEGENERATION': 'VARCHAR',
          'DISPATCHABLELOAD': 'VARCHAR',
          'NETINTERCHANGE': 'VARCHAR',
          'EXCESSGENERATION': 'VARCHAR',
          'LOWER5MINDISPATCH': 'VARCHAR',
          'LOWER5MINIMPORT': 'VARCHAR',
          'LOWER5MINLOCALDISPATCH': 'VARCHAR',
          'LOWER5MINLOCALPRICE': 'VARCHAR',
          'LOWER5MINLOCALREQ': 'VARCHAR',
          'LOWER5MINPRICE': 'VARCHAR',
          'LOWER5MINREQ': 'VARCHAR',
          'LOWER5MINSUPPLYPRICE': 'VARCHAR',
          'LOWER60SECDISPATCH': 'VARCHAR',
          'LOWER60SECIMPORT': 'VARCHAR',
          'LOWER60SECLOCALDISPATCH': 'VARCHAR',
          'LOWER60SECLOCALPRICE': 'VARCHAR',
          'LOWER60SECLOCALREQ': 'VARCHAR',
          'LOWER60SECPRICE': 'VARCHAR',
          'LOWER60SECREQ': 'VARCHAR',
          'LOWER60SECSUPPLYPRICE': 'VARCHAR',
          'LOWER6SECDISPATCH': 'VARCHAR',
          'LOWER6SECIMPORT': 'VARCHAR',
          'LOWER6SECLOCALDISPATCH': 'VARCHAR',
          'LOWER6SECLOCALPRICE': 'VARCHAR',
          'LOWER6SECLOCALREQ': 'VARCHAR',
          'LOWER6SECPRICE': 'VARCHAR',
          'LOWER6SECREQ': 'VARCHAR',
          'LOWER6SECSUPPLYPRICE': 'VARCHAR',
          'RAISE5MINDISPATCH': 'VARCHAR',
          'RAISE5MINIMPORT': 'VARCHAR',
          'RAISE5MINLOCALDISPATCH': 'VARCHAR',
          'RAISE5MINLOCALPRICE': 'VARCHAR',
          'RAISE5MINLOCALREQ': 'VARCHAR',
          'RAISE5MINPRICE': 'VARCHAR',
          'RAISE5MINREQ': 'VARCHAR',
          'RAISE5MINSUPPLYPRICE': 'VARCHAR',
          'RAISE60SECDISPATCH': 'VARCHAR',
          'RAISE60SECIMPORT': 'VARCHAR',
          'RAISE60SECLOCALDISPATCH': 'VARCHAR',
          'RAISE60SECLOCALPRICE': 'VARCHAR',
          'RAISE60SECLOCALREQ': 'VARCHAR',
          'RAISE60SECPRICE': 'VARCHAR',
          'RAISE60SECREQ': 'VARCHAR',
          'RAISE60SECSUPPLYPRICE': 'VARCHAR',
          'RAISE6SECDISPATCH': 'VARCHAR',
          'RAISE6SECIMPORT': 'VARCHAR',
          'RAISE6SECLOCALDISPATCH': 'VARCHAR',
          'RAISE6SECLOCALPRICE': 'VARCHAR',
          'RAISE6SECLOCALREQ': 'VARCHAR',
          'RAISE6SECPRICE': 'VARCHAR',
          'RAISE6SECREQ': 'VARCHAR',
          'RAISE6SECSUPPLYPRICE': 'VARCHAR',
          'AGGREGATEDISPATCHERROR': 'VARCHAR',
          'AVAILABLEGENERATION': 'VARCHAR',
          'AVAILABLELOAD': 'VARCHAR',
          'INITIALSUPPLY': 'VARCHAR',
          'CLEAREDSUPPLY': 'VARCHAR',
          'LOWERREGIMPORT': 'VARCHAR',
          'LOWERREGLOCALDISPATCH': 'VARCHAR',
          'LOWERREGLOCALREQ': 'VARCHAR',
          'LOWERREGREQ': 'VARCHAR',
          'RAISEREGIMPORT': 'VARCHAR',
          'RAISEREGLOCALDISPATCH': 'VARCHAR',
          'RAISEREGLOCALREQ': 'VARCHAR',
          'RAISEREGREQ': 'VARCHAR',
          'RAISE5MINLOCALVIOLATION': 'VARCHAR',
          'RAISEREGLOCALVIOLATION': 'VARCHAR',
          'RAISE60SECLOCALVIOLATION': 'VARCHAR',
          'RAISE6SECLOCALVIOLATION': 'VARCHAR',
          'LOWER5MINLOCALVIOLATION': 'VARCHAR',
          'LOWERREGLOCALVIOLATION': 'VARCHAR',
          'LOWER60SECLOCALVIOLATION': 'VARCHAR',
          'LOWER6SECLOCALVIOLATION': 'VARCHAR',
          'RAISE5MINVIOLATION': 'VARCHAR',
          'RAISEREGVIOLATION': 'VARCHAR',
          'RAISE60SECVIOLATION': 'VARCHAR',
          'RAISE6SECVIOLATION': 'VARCHAR',
          'LOWER5MINVIOLATION': 'VARCHAR',
          'LOWERREGVIOLATION': 'VARCHAR',
          'LOWER60SECVIOLATION': 'VARCHAR',
          'LOWER6SECVIOLATION': 'VARCHAR',
          'RAISE6SECRRP': 'VARCHAR',
          'RAISE6SECROP': 'VARCHAR',
          'RAISE6SECAPCFLAG': 'VARCHAR',
          'RAISE60SECRRP': 'VARCHAR',
          'RAISE60SECROP': 'VARCHAR',
          'RAISE60SECAPCFLAG': 'VARCHAR',
          'RAISE5MINRRP': 'VARCHAR',
          'RAISE5MINROP': 'VARCHAR',
          'RAISE5MINAPCFLAG': 'VARCHAR',
          'RAISEREGRRP': 'VARCHAR',
          'RAISEREGROP': 'VARCHAR',
          'RAISEREGAPCFLAG': 'VARCHAR',
          'LOWER6SECRRP': 'VARCHAR',
          'LOWER6SECROP': 'VARCHAR',
          'LOWER6SECAPCFLAG': 'VARCHAR',
          'LOWER60SECRRP': 'VARCHAR',
          'LOWER60SECROP': 'VARCHAR',
          'LOWER60SECAPCFLAG': 'VARCHAR',
          'LOWER5MINRRP': 'VARCHAR',
          'LOWER5MINROP': 'VARCHAR',
          'LOWER5MINAPCFLAG': 'VARCHAR',
          'LOWERREGRRP': 'VARCHAR',
          'LOWERREGROP': 'VARCHAR',
          'LOWERREGAPCFLAG': 'VARCHAR',
          'RAISE6SECACTUALAVAILABILITY': 'VARCHAR',
          'RAISE60SECACTUALAVAILABILITY': 'VARCHAR',
          'RAISE5MINACTUALAVAILABILITY': 'VARCHAR',
          'RAISEREGACTUALAVAILABILITY': 'VARCHAR',
          'LOWER6SECACTUALAVAILABILITY': 'VARCHAR',
          'LOWER60SECACTUALAVAILABILITY': 'VARCHAR',
          'LOWER5MINACTUALAVAILABILITY': 'VARCHAR',
          'LOWERREGACTUALAVAILABILITY': 'VARCHAR',
          'LORSURPLUS': 'VARCHAR',
          'LRCSURPLUS': 'VARCHAR'
        },
        filename = 1,
        null_padding = true,
        ignore_errors = 1,
        auto_detect = false
      )
    WHERE
      I = 'D'
      AND UNIT = 'DREGION'
      AND VERSION = '3'
  ;

SELECT '[DUID] Start processing - fetching from 4 web sources (this may take time)...' AS Status;

create or replace temp table duid_staging as
WITH
  duid_aemo AS MATERIALIZED (
    SELECT
      DUID AS DUID,
      first(Region) AS Region,
      first("Fuel Source - Descriptor") AS FuelSourceDescriptor,
      first(Participant) AS Participant
    FROM
      read_csv('https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/djouallah-patch-1/duid_data.csv')
    WHERE
      length(DUID) > 2
    GROUP BY
      DUID
  ),
  states AS (
    SELECT
      'WA1' AS RegionID,
      'Western Australia' AS States
    UNION ALL
    SELECT
      'QLD1',
      'Queensland'
    UNION ALL
    SELECT
      'NSW1',
      'New South Wales'
    UNION ALL
    SELECT
      'TAS1',
      'Tasmania'
    UNION ALL
    SELECT
      'SA1',
      'South Australia'
    UNION ALL
    SELECT
      'VIC1',
      'Victoria'
  ),
  x AS MATERIALIZED (
    SELECT
      'WA1' AS Region,
      "Facility Code" AS DUID,
      "Participant Name" AS Participant
    FROM
      read_csv_auto('https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv')
  ),
  tt AS MATERIALIZED (
    SELECT
      *
    FROM
      read_csv_auto('https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/main/WA_ENERGY.csv', header = 1)
  ),
  duid_wa AS (
    SELECT
      x.DUID,
      x.Region,
      Technology AS FuelSourceDescriptor,
      x.Participant
    FROM
      x
      LEFT JOIN tt ON x.DUID = tt.DUID
  ),
  duid_all AS (
    SELECT
      *
    FROM
      duid_aemo
    UNION ALL
    SELECT
      *
    FROM
      duid_wa
  ),
  geo as MATERIALIZED  (
    select
      duid,
      max(latitude) as latitude,
      max(longitude) as longitude
    from
      read_csv(
        'https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/main/geo_data.csv'
      )
    where
      latitude is not null
    group by all
  )
SELECT
  a.DUID,
  Region,
  UPPER(LEFT(TRIM(FuelSourceDescriptor), 1)) || LOWER(SUBSTR(TRIM(FuelSourceDescriptor), 2)) AS FuelSourceDescriptor,
  Participant,
  states.States AS State,
  geo.latitude,
  geo.longitude
FROM
  duid_all a
  JOIN states ON a.Region = states.RegionID
  left JOIN geo ON a.duid = geo.duid;

SELECT '[DUID] Loaded ' || count(*) || ' facilities from web sources' AS Status FROM duid_staging;
  
create or replace temp view  summary_staging as
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

INSERT INTO
  scada BY NAME
SELECT
  UNIT,
  DUID,
  CAST(columns (* EXCLUDE (DUID, UNIT, SETTLEMENTDATE, I, XX, filename)) AS DOUBLE),
  parse_filename(filename) AS FILE,
  CAST(SETTLEMENTDATE AS TIMESTAMP) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT16) AS YEAR
FROM
  scada_staging ;

SELECT '[INSERT] Inserted ' || count(*) || ' SCADA records' AS Status FROM scada WHERE date >= current_date - interval '7 days';

INSERT INTO
  price BY NAME
SELECT
  UNIT,
  REGIONID,
  CAST(columns (* EXCLUDE (REGIONID, UNIT, SETTLEMENTDATE, I, XX, filename)) AS DOUBLE),
  parse_filename(filename) AS FILE,
  CAST(SETTLEMENTDATE AS TIMESTAMP) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT16) AS YEAR
FROM
  price_staging ;

SELECT '[INSERT] Inserted ' || count(*) || ' PRICE records' AS Status FROM price WHERE date >= current_date - interval '7 days';

SELECT '[REFRESH] Refreshing dimension tables...' AS Status;

BEGIN TRANSACTION;
truncate duid     ;insert into duid select * from duid_staging ;
truncate summary  ;insert into summary BY NAME select * from summary_staging ;
COMMIT;

SELECT '[COMPLETE] Loaded ' || count(*) || ' DUIDs' AS Status FROM duid;
SELECT '[COMPLETE] Loaded ' || count(*) || ' summary records' AS Status FROM summary;
------------------------------------------------------------

-- verify data load
SELECT '[VERIFY] Latest data timestamp: ' || max(cutoff) AS Status FROM summary;


-----------------------------------------------------------------
--checkpoint;
use memory ;
detach dwh ;
------------------------------------------------------------------