-- SCADA Today Table
-- This table is used to store the SCADA data for today only.
CREATE TABLE IF NOT EXISTS scada_today(
  DUID VARCHAR,
  INITIALMW DOUBLE,
  INTERVENTION DOUBLE,
  SETTLEMENTDATE TIMESTAMP WITH TIME ZONE,
  date DATE,
  file VARCHAR,
  PRIORITY INTEGER,
  "YEAR" BIGINT
);

ALTER TABLE if exists scada_today SET PARTITIONED BY (YEAR);

SET VARIABLE list_of_files_scada_today = (
                                          WITH xxxx AS (
                                               SELECT
                                                concat('/lakehouse/default/Files/', extracted_filepath) AS file
                                                FROM '{scada_today_path}'
                                                WHERE parse_filename(extracted_filepath) NOT IN (SELECT DISTINCT file FROM scada_today)
                                                ORDER BY file
                                                LIMIT {Nbr_Files_to_Process}
                                                   )
                                           SELECT list(file) FROM xxxx 
  );

insert into scada_today by name
WITH raw AS (
  FROM read_csv(getvariable('list_of_files_scada_today'),
    Skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {{
      'I' : 'VARCHAR',
      'DISPATCH' : 'VARCHAR',
      'UNIT_SCADA' : 'VARCHAR',
      'xx' : 'VARCHAR',
      'SETTLEMENTDATE' : 'timestamp',
      'DUID' : 'VARCHAR',
      'SCADAVALUE' : 'double',
      'LASTCHANGED' : 'timestamp'
    }},
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false
  )
  WHERE I = 'D'
    AND SCADAVALUE != 0
)
SELECT
  DUID,
  SCADAVALUE AS INITIALMW,
  CAST(0 AS double) AS INTERVENTION,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS date) AS date,
  parse_filename(filename) AS file,
  0 AS PRIORITY,
  isoyear(CAST(SETTLEMENTDATE AS timestamp)) AS YEAR
FROM raw order by date
