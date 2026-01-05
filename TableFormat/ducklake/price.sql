
----------------- Process PRICE data from OneLake archive (daily files) --------------------------------------

-------------------------------------------------------------

---------------------------------------------------- process price data from OneLake archive

-- Get list of archived daily files not yet processed into price table
-- Path format: /daily/year=YYYY/source_file=FILENAME/data_0.zip
SET VARIABLE list_of_files_price = (
  SELECT COALESCE(list('zip://' || getvariable('csv_archive_path') || '/daily/year=' || substring(source_filename, 14, 4) || '/source_file=' || source_filename || '/data_0.zip/*.CSV'), [])
  FROM (
    SELECT source_filename
    FROM csv_archive_log
    WHERE source_type = 'daily'
      AND source_filename NOT IN (SELECT DISTINCT split_part(file, '.', 1) FROM price)
    LIMIT getvariable('row_limit')
  )
);

-- Set flags for conditional processing
SET VARIABLE has_new_price = (SELECT len(getvariable('list_of_files_price')) > 0);

SELECT '[PRICE] Processing ' || len(getvariable('list_of_files_price')) || ' files from OneLake' AS Status;

-------------------------------------- staging area for price data

create or replace temp view price_staging as
  
    SELECT
      *
    FROM
      read_csv(
        getvariable('list_of_files_price'),
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
        auto_detect = false,
        hive_partitioning = false
      )
    WHERE
      I = 'D'
      AND UNIT = 'DREGION'
      AND VERSION = '3'
  ;

-------------------------------------- insert processed data

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

------------------------------------------------------------------------------

INSERT INTO calendar
SELECT cast(unnest(generate_series(cast ('2018-04-01' as date), cast('2026-12-31' as date), interval 1 day)) as date) as date,
EXTRACT(year from date) as year,
EXTRACT(month from date) as month
WHERE NOT EXISTS (SELECT 1 FROM calendar);
------------------------------------------------------------------------------

SELECT '[COMPLETE] PRICE processing finished' AS Status;
