
----------------- Process SCADA data from OneLake archive (daily files) --------------------------------------

SELECT '[CONFIG] Processing ' || getvariable('row_limit') || ' files per batch' AS Status;

-------------------------------------------------------------

---------------------------------------------------- process scada data from OneLake archive

-- Get list of archived daily files not yet processed into scada table
-- Path format: /daily/year=YYYY/source_file=FILENAME/data_0.zip
SET VARIABLE list_of_files_scada_daily = (
  SELECT COALESCE(list('zip://' || getvariable('csv_archive_path') || '/daily/year=' || substring(source_filename, 14, 4) || '/source_file=' || source_filename || '/data_0.zip/*.CSV'), [])
  FROM (
    SELECT source_filename
    FROM csv_archive_log
    WHERE source_type = 'daily'
      AND source_filename NOT IN (SELECT DISTINCT split_part(file, '.', 1) FROM scada)
    LIMIT getvariable('row_limit')
  )
);

-- Set flags for conditional processing
SET VARIABLE has_new_scada_daily = (SELECT len(getvariable('list_of_files_scada_daily')) > 0);

SELECT '[SCADA] Processing ' || len(getvariable('list_of_files_scada_daily')) || ' files from OneLake' AS Status;

-------------------------------------- staging area for scada data

create or replace temp view scada_staging as
  
    SELECT
      *
    FROM
      read_csv(
        getvariable('list_of_files_scada_daily'),
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
        auto_detect = false,
        hive_partitioning = false
      )
    WHERE
      I = 'D'
      AND UNIT = 'DUNIT'
      AND VERSION = '3'
  ;

-------------------------------------- insert processed data

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

------------------------------------------------------------------------------

SELECT '[COMPLETE] SCADA processing finished' AS Status;
