
---------------------------------------------------- process scada_today data from OneLake archive

-- Get list of archived intraday SCADA files not yet processed
-- Path format: /scada_today/day=YYYYMMDD/source_file=FILENAME/data_0.zip
SET VARIABLE list_of_files_scada_today = (
  SELECT COALESCE(list('zip://' || getvariable('csv_archive_path') || '/scada_today/day=' || substring(source_filename, 22, 8) || '/source_file=' || source_filename || '/data_0.zip/*.CSV'), [])
  FROM csv_archive_log
  WHERE source_type = 'scada_today'
    AND source_filename NOT IN (SELECT DISTINCT split_part(file, '.', 1) FROM scada_today)
);

SELECT '[SCADA_TODAY] Processing ' || len(getvariable('list_of_files_scada_today')) || ' files from OneLake' AS Status;

-------------------------------------- staging area for scada data

create or replace temp view scada_staging as
  
    SELECT
      *
    FROM
      read_csv(
        getvariable('list_of_files_scada_today'),
    skip = 1,
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
    auto_detect = false,
    hive_partitioning = false
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

SELECT '[COMPLETE] SCADA_TODAY processing finished' AS Status;
