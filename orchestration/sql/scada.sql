CREATE VIEW if not exists scada(file) AS SELECT 'dummy';
SET VARIABLE list_of_files_scada =
(
  WITH xxxx AS (
    SELECT
      concat('abfss://udf@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Files/', extracted_filepath) AS file
    FROM 'abfss://udf@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Files/Reports/Current/Daily_Reports/download_log.csv'
    WHERE parse_filename(extracted_filepath) NOT IN (SELECT DISTINCT file FROM scada)
    ORDER BY file
    -- LIMIT 1000
  )
  SELECT list(file) FROM xxxx
);
WITH raw AS (
    FROM read_csv(
        getvariable('list_of_files_scada'),
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
        AND VERSION = 3
)
SELECT
    UNIT,
    DUID,
    CAST(columns(* EXCLUDE (DUID, UNIT, SETTLEMENTDATE, I, XX, filename)) AS DOUBLE),
    parse_filename(filename) AS file,
    CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
    CAST(SETTLEMENTDATE AS DATE) AS date,
    YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS YEAR
FROM raw
