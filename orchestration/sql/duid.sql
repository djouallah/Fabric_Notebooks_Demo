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
  ),
geo as(
  select duid, max(latitude) as latitude,max(longitude) as longitude from
  read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vR_I3U-f2DtY4ex8QXV_S1T19JYy58__nz52Ra6Mm10r3_vJik5OrvQecN-pFWfjUbIE6m0wMl_R6kL/pub?gid=0&single=true&output=csv')

  where latitude is not null
group by all)
SELECT
  a.DUID,
  Region,
  UPPER(LEFT(TRIM(FuelSourceDescriptor), 1)) || LOWER(SUBSTR(TRIM(FuelSourceDescriptor), 2)) AS FuelSourceDescriptor,
  Participant,
  states.States AS State,
  geo.latitude,
  geo.longitude
FROM duid_all a
JOIN states ON a.Region = states.RegionID
left JOIN geo ON a.duid = geo.duid;
