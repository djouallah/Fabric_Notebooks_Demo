
---------------------------------------------------- process price_today data from OneLake archive

-- Get list of archived intraday PRICE files not yet processed
-- Path format: /price_today/day=YYYYMMDD/source_file=FILENAME/data_0.zip
SET VARIABLE list_of_files_price = (
  SELECT COALESCE(list('zip://' || getvariable('csv_archive_path') || '/price_today/day=' || substring(source_filename, 19, 8) || '/source_file=' || source_filename || '/data_0.zip/*.CSV'), [])
  FROM csv_archive_log
  WHERE source_type = 'price_today'
    AND source_filename NOT IN (SELECT DISTINCT split_part(file, '.', 1) FROM price_today)
);

SELECT '[PRICE_TODAY] Processing ' || len(getvariable('list_of_files_price')) || ' files from OneLake' AS Status;

-------------------------------------- staging area for price data

create or replace temp view price_staging as
  
WITH RAW_price AS (
    SELECT *
    FROM read_csv(
      getvariable('list_of_files_price'),
      skip = 1,
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
      auto_detect = false,
      hive_partitioning = false
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

SELECT '[COMPLETE] PRICE_TODAY processing finished' AS Status;
