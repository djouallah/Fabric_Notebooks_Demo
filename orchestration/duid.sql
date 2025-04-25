-- materialized: (duid,overwrite)
WITH
  duid_aemo AS (
    SELECT
      DUID AS DUID,
      first(Region) AS Region,
      first("Fuel Source - Descriptor") AS FuelSourceDescriptor,
      first(Participant) AS Participant
    FROM 'abfss://udf@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Files/raw/nem-registration-and-exemption-list_PU_and_Scheduled_Loads.csv',
    
    WHERE
      length(DUID) > 2
    GROUP BY
      DUID
  ),
  states AS (
    SELECT 'WA1' AS RegionID, 'Western Australia' AS States
    UNION ALL SELECT 'QLD1', 'Queensland'
    UNION ALL SELECT 'NSW1', 'New South Walles'
    UNION ALL SELECT 'TAS1', 'Tasmania'
    UNION ALL SELECT 'SA1', 'South Australia'
    UNION ALL SELECT 'VIC1', 'Victoria'
  ),
  x AS (
    SELECT
      'WA1' AS Region,
      "Facility Code" AS DUID,
      "Participant Name" AS Participant
    FROM read_csv_auto('https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv')
  ),
  tt AS (
    SELECT
      *
    FROM read_csv_auto('https://github.com/djouallah/aemo_fabric/raw/main/WA_ENERGY.csv', header = 1)
  ),
  duid_wa AS (
    SELECT
      x.DUID,
      x.Region,
      Technology AS FuelSourceDescriptor,
      x.Participant
    FROM x
    LEFT JOIN tt ON x.DUID = tt.DUID
  ),
  duid_all AS (
    SELECT
      *
    FROM duid_aemo
    UNION ALL
    SELECT
      *
    FROM duid_wa
  )
SELECT
  DUID,
  Region,
  FuelSourceDescriptor,
  Participant,
  states.States AS State
FROM duid_all a
JOIN states ON a.Region = states.RegionID;
