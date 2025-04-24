-- materialized: (price_today,append)
CREATE VIEW if not exists price_today(file) AS SELECT 'dummy';
SET VARIABLE list_of_files_price_today =
(
  WITH xxxx AS (
    SELECT
      concat('abfss://udf@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Files/', extracted_filepath) AS file
    FROM 'abfss://udf@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Files/Reports/Current/DispatchIS_Reports/download_log.csv'
    WHERE parse_filename(extracted_filepath) NOT IN (SELECT DISTINCT file FROM price_today)
    ORDER BY file
    LIMIT 5000
  )
  SELECT list(file) FROM xxxx
);

WITH RAW_price AS (
    FROM read_csv(getvariable('list_of_files_price_today'),
      Skip = 1,
      header = 0,
      all_varchar = 1,
      columns = {
        'I' : 'VARCHAR',
        'DISPATCH' : 'VARCHAR',
        'PRICE' : 'VARCHAR',
        'xx' : 'VARCHAR',
        'SETTLEMENTDATE' : 'VARCHAR',
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
    CAST(columns(* EXCLUDE (SETTLEMENTDATE, REGIONID, I,xx, PRICE, filename, OCD_STATUS, MII_STATUS, DISPATCH, PRICE_STATUS, LASTCHANGED)) AS DOUBLE), -- Cast remaining columns to DOUBLE
    CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
    CAST(SETTLEMENTDATE AS date) AS date,
    parse_filename(filename) AS file,
    year(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) AS YEAR
  FROM RAW_price
  ORDER BY date