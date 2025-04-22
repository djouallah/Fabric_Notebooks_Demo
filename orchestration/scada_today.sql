-- materialized: (scada_today,parquet,append)

WITH raw AS (
  FROM read_csv(getvariable('list_of_files_scada_today'),
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