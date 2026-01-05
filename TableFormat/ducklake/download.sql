-- ============================================================================
-- ELECTRICITY DATA PIPELINE - RAW CSV ARCHIVE
-- ============================================================================
-- Archives raw CSV files from AEMO to Azure before processing.
-- Uses csv_archive_log to track what's been archived (avoids re-downloading).
-- ============================================================================



SET VARIABLE csv_archive_path = getvariable('PATH_ROOT') || '/Files/csv';

-- ============================================================================
-- DAILY REPORTS (SCADA + PRICE)
-- ============================================================================

CREATE OR REPLACE TEMP TABLE daily_files_web AS
WITH
  html_data AS (
    SELECT content AS html
    FROM read_text('https://nemweb.com.au/Reports/Current/Daily_Reports')
  ),
  lines AS (
    SELECT unnest(string_split(html, '<br>')) AS line
    FROM html_data
  )
SELECT
  'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
  split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\.zip)"', 1), '.', 1) AS filename
FROM lines
WHERE line LIKE '%PUBLIC_DAILY%'
ORDER BY full_url DESC;

-- Filter to files not yet archived - save the list ONCE to ensure consistency
CREATE OR REPLACE TEMP TABLE daily_to_archive AS
SELECT full_url, filename
FROM daily_files_web
WHERE filename NOT IN (SELECT source_filename FROM csv_archive_log WHERE source_type = 'daily')
LIMIT getvariable('download_limit');

SET VARIABLE has_daily_to_archive = (SELECT count(*) > 0 FROM daily_to_archive);
SET VARIABLE daily_urls = (SELECT COALESCE(NULLIF(list(full_url), []), list_value('')) FROM daily_to_archive);
SET VARIABLE daily_expected = (SELECT count(*) FROM daily_to_archive);
SET VARIABLE daily_paths = (SELECT COALESCE(NULLIF(list(getvariable('csv_archive_path') || '/daily/year=' || CAST(substring(split_part(filename, '_', 3), 1, 4) AS INT) || '/source_file=' || filename || '/data_0.zip'), []), list_value('')) FROM daily_to_archive);

SELECT '[DOWNLOAD] Found ' || getvariable('daily_expected') || ' daily files to archive' AS Status;

-- Archive raw daily ZIP files
COPY (
  SELECT 
    content,
    split_part(split_part(filename, '/', -1), '.', 1) AS source_file,
    CAST(substring(split_part(split_part(filename, '/', -1), '_', 3), 1, 4) AS INT) AS year
  FROM read_blob(getvariable('daily_urls'))
  WHERE getvariable('has_daily_to_archive') AND filename != ''
) TO (getvariable('csv_archive_path') || '/daily') 
(FORMAT BLOB, PARTITION_BY (year, source_file), FILE_EXTENSION 'zip', OVERWRITE_OR_IGNORE);

-- Only log if all expected files exist
INSERT INTO csv_archive_log BY NAME
SELECT 
  'daily' AS source_type,
  filename AS source_filename,
  '/daily/year=' || CAST(substring(split_part(filename, '_', 3), 1, 4) AS INT) || '/source_file=' || filename || '/data_0.zip' AS archive_path,
  now() AS archived_at
FROM daily_to_archive
WHERE getvariable('daily_expected') > 0
  AND (SELECT count(*) FROM glob(getvariable('daily_paths'))) = getvariable('daily_expected');

-- ============================================================================
-- INTRADAY SCADA (5-minute dispatch)
-- ============================================================================

CREATE OR REPLACE TEMP TABLE intraday_scada_web AS
WITH
  html_data AS (
    SELECT content AS html
    FROM read_text('http://nemweb.com.au/Reports/Current/Dispatch_SCADA/')
  ),
  lines AS (
    SELECT unnest(string_split(html, '<br>')) AS line
    FROM html_data
  )
SELECT
  'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
  split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\.zip)"', 1), '.', 1) AS filename
FROM lines
WHERE line LIKE '%PUBLIC_DISPATCHSCADA%'
ORDER BY full_url DESC
LIMIT 500;

CREATE OR REPLACE TEMP TABLE scada_today_to_archive AS
SELECT full_url, filename
FROM intraday_scada_web
WHERE filename NOT IN (SELECT source_filename FROM csv_archive_log WHERE source_type = 'scada_today')
LIMIT getvariable('download_limit');

SET VARIABLE has_scada_today_to_archive = (SELECT count(*) > 0 FROM scada_today_to_archive);
SET VARIABLE scada_urls = (SELECT COALESCE(NULLIF(list(full_url), []), list_value('')) FROM scada_today_to_archive);
SET VARIABLE scada_expected = (SELECT count(*) FROM scada_today_to_archive);
SET VARIABLE scada_paths = (SELECT COALESCE(NULLIF(list(getvariable('csv_archive_path') || '/scada_today/day=' || substring(split_part(filename, '_', 3), 1, 8) || '/source_file=' || filename || '/data_0.zip'), []), list_value('')) FROM scada_today_to_archive);

SELECT '[DOWNLOAD] Found ' || getvariable('scada_expected') || ' intraday SCADA files to archive' AS Status;

-- Archive SCADA files
COPY (
  SELECT 
    content,
    split_part(split_part(filename, '/', -1), '.', 1) AS source_file,
    substring(split_part(split_part(filename, '/', -1), '_', 3), 1, 8) AS day
  FROM read_blob(getvariable('scada_urls'))
  WHERE getvariable('has_scada_today_to_archive') AND filename != ''
) TO (getvariable('csv_archive_path') || '/scada_today') 
(FORMAT BLOB, PARTITION_BY (day, source_file), FILE_EXTENSION 'zip', OVERWRITE_OR_IGNORE);

-- Only log if all expected files exist
INSERT INTO csv_archive_log BY NAME
SELECT 
  'scada_today' AS source_type,
  filename AS source_filename,
  '/scada_today/day=' || substring(split_part(filename, '_', 3), 1, 8) || '/source_file=' || filename || '/data_0.zip' AS archive_path,
  now() AS archived_at
FROM scada_today_to_archive
WHERE getvariable('scada_expected') > 0
  AND (SELECT count(*) FROM glob(getvariable('scada_paths'))) = getvariable('scada_expected');

-- ============================================================================
-- INTRADAY PRICE (5-minute dispatch)
-- ============================================================================

CREATE OR REPLACE TEMP TABLE intraday_price_web AS
WITH
  html_data AS (
    SELECT content AS html
    FROM read_text('http://nemweb.com.au/Reports/Current/DispatchIS_Reports/')
  ),
  lines AS (
    SELECT unnest(string_split(html, '<br>')) AS line
    FROM html_data
  )
SELECT
  'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
  split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\.zip)"', 1), '.', 1) AS filename
FROM lines
WHERE line LIKE '%PUBLIC_DISPATCHIS_%.zip%'
ORDER BY full_url DESC
LIMIT 500;

CREATE OR REPLACE TEMP TABLE price_today_to_archive AS
SELECT full_url, filename
FROM intraday_price_web
WHERE filename NOT IN (SELECT source_filename FROM csv_archive_log WHERE source_type = 'price_today')
LIMIT getvariable('download_limit');

SET VARIABLE has_price_today_to_archive = (SELECT count(*) > 0 FROM price_today_to_archive);
SET VARIABLE price_urls = (SELECT COALESCE(NULLIF(list(full_url), []), list_value('')) FROM price_today_to_archive);
SET VARIABLE price_expected = (SELECT count(*) FROM price_today_to_archive);
SET VARIABLE price_paths = (SELECT COALESCE(NULLIF(list(getvariable('csv_archive_path') || '/price_today/day=' || substring(split_part(filename, '_', 3), 1, 8) || '/source_file=' || filename || '/data_0.zip'), []), list_value('')) FROM price_today_to_archive);

SELECT '[DOWNLOAD] Found ' || getvariable('price_expected') || ' intraday PRICE files to archive' AS Status;

-- Archive PRICE files
COPY (
  SELECT 
    content,
    split_part(split_part(filename, '/', -1), '.', 1) AS source_file,
    substring(split_part(split_part(filename, '/', -1), '_', 3), 1, 8) AS day
  FROM read_blob(getvariable('price_urls'))
  WHERE getvariable('has_price_today_to_archive') AND filename != ''
) TO (getvariable('csv_archive_path') || '/price_today') 
(FORMAT BLOB, PARTITION_BY (day, source_file), FILE_EXTENSION 'zip', OVERWRITE_OR_IGNORE);

-- Only log if all expected files exist
INSERT INTO csv_archive_log BY NAME
SELECT 
  'price_today' AS source_type,
  filename AS source_filename,
  '/price_today/day=' || substring(split_part(filename, '_', 3), 1, 8) || '/source_file=' || filename || '/data_0.zip' AS archive_path,
  now() AS archived_at
FROM price_today_to_archive
WHERE getvariable('price_expected') > 0
  AND (SELECT count(*) FROM glob(getvariable('price_paths'))) = getvariable('price_expected');

-- ============================================================================
-- DUID REFERENCE DATA (from GitHub and WA AEMO) - always refresh
-- ============================================================================

SELECT '[DOWNLOAD] Refreshing 4 DUID reference files...' AS Status;

-- Archive DUID files - simple COPY without RETURN_FILES
COPY (
  SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/djouallah-patch-1/duid_data.csv', null_padding=true, ignore_errors=true)
) TO (getvariable('csv_archive_path') || '/duid/duid_data.csv') (FORMAT CSV, HEADER);

COPY (
  SELECT * FROM read_csv_auto('https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv', null_padding=true, ignore_errors=true)
) TO (getvariable('csv_archive_path') || '/duid/facilities.csv') (FORMAT CSV, HEADER);

COPY (
  SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/main/WA_ENERGY.csv', header=true, null_padding=true, ignore_errors=true)
) TO (getvariable('csv_archive_path') || '/duid/WA_ENERGY.csv') (FORMAT CSV, HEADER);

COPY (
  SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/djouallah/aemo_fabric/refs/heads/main/geo_data.csv', null_padding=true, ignore_errors=true)
) TO (getvariable('csv_archive_path') || '/duid/geo_data.csv') (FORMAT CSV, HEADER);

-- Log archived DUID files
DELETE FROM csv_archive_log WHERE source_type LIKE 'duid_%';

INSERT INTO csv_archive_log BY NAME
SELECT * FROM (VALUES
  ('duid_data', 'duid_data', '/duid/duid_data.csv', now()),
  ('duid_facilities', 'facilities', '/duid/facilities.csv', now()),
  ('duid_wa_energy', 'WA_ENERGY', '/duid/WA_ENERGY.csv', now()),
  ('duid_geo_data', 'geo_data', '/duid/geo_data.csv', now())
) AS t(source_type, source_filename, archive_path, archived_at);

SELECT '[DOWNLOAD] DUID files written: 4' AS Status;

SELECT '[DOWNLOAD COMPLETE] Archived files summary:' AS Status;
SELECT source_type, count(*) as file_count, max(archived_at) as last_archived 
FROM csv_archive_log 
GROUP BY source_type;
