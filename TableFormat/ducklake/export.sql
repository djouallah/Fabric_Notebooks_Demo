-- verify data load
SELECT '[VERIFY] Latest data timestamp: ' || max(cutoff) AS Status FROM summary;

SELECT '[CHECKPOINT] Exporting inline data to Parquet files, compaction etc ...' AS Status;
CALL ducklake_flush_inlined_data('dwh');
checkpoint;

SELECT 'Export Delta Metadata' AS Status;

use  __ducklake_metadata_dwh  ; 

-- Create export tracking table if it doesn't exist
CREATE TABLE IF NOT EXISTS ducklake_export_log (
    table_id BIGINT,
    snapshot_id BIGINT,
    export_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (table_id, snapshot_id)
);


-- 3. Main export logic
CREATE OR REPLACE TEMP TABLE export_summary (
    table_id BIGINT,
    schema_name VARCHAR,
    table_name VARCHAR,
    table_root VARCHAR,
    status VARCHAR,
    snapshot_id BIGINT,
    message VARCHAR
);

INSERT INTO export_summary
WITH 
data_root_config AS (
    SELECT value AS data_root FROM ducklake_metadata WHERE key = 'data_path'
),
active_tables AS (
    SELECT 
        t.table_id,
        t.table_name,
        s.schema_name,
        t.path AS table_path,
        s.path AS schema_path,
        rtrim((SELECT data_root FROM data_root_config), '/') || '/' || 
            CASE 
                WHEN trim(s.path, '/') != '' THEN trim(s.path, '/') || '/' 
                ELSE '' 
            END || 
            trim(t.path, '/') AS table_root
    FROM ducklake_table t
    JOIN ducklake_schema s USING(schema_id)
    WHERE t.end_snapshot IS NULL
),
latest_snapshots AS (
    SELECT 
        t.*,
        (SELECT MAX(sc.snapshot_id)
         FROM ducklake_snapshot_changes sc
         WHERE sc.changes_made LIKE '%inserted_into_table:' || t.table_id || '%'
            OR sc.changes_made LIKE '%deleted_from_table:' || t.table_id || '%'
            OR sc.changes_made LIKE '%compacted_table:' || t.table_id || '%'
            OR sc.changes_made LIKE '%altered_table:' || t.table_id || '%'
        ) AS latest_snapshot
    FROM active_tables t
),
export_status AS (
    SELECT 
        ls.*,
        el.snapshot_id AS exported_snapshot,
        (SELECT COUNT(*) FROM ducklake_data_file df 
         WHERE df.table_id = ls.table_id 
           AND df.begin_snapshot <= ls.latest_snapshot
           AND (df.end_snapshot IS NULL OR df.end_snapshot > ls.latest_snapshot)
        ) AS file_count
    FROM latest_snapshots ls
    LEFT JOIN ducklake_export_log el 
        ON ls.table_id = el.table_id 
        AND ls.latest_snapshot = el.snapshot_id
)
SELECT 
    table_id,
    schema_name,
    table_name,
    table_root,
    CASE 
        WHEN latest_snapshot IS NULL THEN 'no_snapshots'
        WHEN file_count = 0 THEN 'no_data_files'
        WHEN exported_snapshot IS NOT NULL THEN 'already_exported'
        ELSE 'needs_export'
    END AS status,
    latest_snapshot,
    CASE 
        WHEN latest_snapshot IS NULL THEN 'No snapshots found'
        WHEN file_count = 0 THEN 'Table has no data files (empty table)'
        WHEN exported_snapshot IS NOT NULL THEN 'Snapshot ' || latest_snapshot || ' already exported'
        ELSE 'Ready for export'
    END AS message
FROM export_status;

-- 4. Generate checkpoint files for tables that need export  
CREATE OR REPLACE TEMP TABLE temp_checkpoint_parquet AS
WITH 
tables_to_export AS (
    -- Only export tables that have at least one data file
    SELECT es.* 
    FROM export_summary es
    WHERE es.status = 'needs_export'
      AND EXISTS (
          SELECT 1 FROM ducklake_data_file df 
          WHERE df.table_id = es.table_id 
            AND df.begin_snapshot <= es.snapshot_id
            AND (df.end_snapshot IS NULL OR df.end_snapshot > es.snapshot_id)
      )
),
table_schemas AS (
    SELECT 
        te.table_id,
        te.schema_name,
        te.table_name,
        te.snapshot_id,
        te.table_root,
        list({
            'name': c.column_name,
            'type': 
                CASE
                    WHEN contains(lower(c.column_type), 'int') AND contains(c.column_type, '64') THEN 'long'
                    WHEN contains(lower(c.column_type), 'int') THEN 'integer'
                    WHEN contains(lower(c.column_type), 'float') THEN 'double'
                    WHEN contains(lower(c.column_type), 'double') THEN 'double'
                    WHEN contains(lower(c.column_type), 'bool') THEN 'boolean'
                    WHEN contains(lower(c.column_type), 'timestamp') THEN 'timestamp'
                    WHEN contains(lower(c.column_type), 'date') THEN 'date'
                    WHEN contains(lower(c.column_type), 'decimal') THEN lower(c.column_type)
                    ELSE 'string'
                END,
            'nullable': true,
            'metadata': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(name VARCHAR, type VARCHAR, nullable BOOLEAN, metadata MAP(VARCHAR, VARCHAR)) ORDER BY c.column_order) AS schema_fields
    FROM tables_to_export te
    JOIN ducklake_table t ON te.table_id = t.table_id
    JOIN ducklake_column c ON t.table_id = c.table_id
    WHERE c.begin_snapshot <= te.snapshot_id
      AND (c.end_snapshot IS NULL OR c.end_snapshot > te.snapshot_id)
    GROUP BY te.table_id, te.schema_name, te.table_name, te.snapshot_id, te.table_root
),
file_column_stats_agg AS (
    SELECT 
        ts.table_id,
        df.data_file_id,
        c.column_name,
        ANY_VALUE(c.column_type) AS column_type,
        MAX(fcs.value_count) AS value_count,
        MIN(fcs.min_value) AS min_value,
        MAX(fcs.max_value) AS max_value,
        MAX(fcs.null_count) AS null_count
    FROM table_schemas ts
    JOIN ducklake_data_file df ON ts.table_id = df.table_id
    LEFT JOIN ducklake_file_column_stats fcs ON df.data_file_id = fcs.data_file_id
    LEFT JOIN ducklake_column c ON fcs.column_id = c.column_id
    WHERE df.begin_snapshot <= ts.snapshot_id
      AND (df.end_snapshot IS NULL OR df.end_snapshot > ts.snapshot_id)
      AND c.column_id IS NOT NULL
      AND c.begin_snapshot <= ts.snapshot_id 
      AND (c.end_snapshot IS NULL OR c.end_snapshot > ts.snapshot_id)
    GROUP BY ts.table_id, df.data_file_id, c.column_name
),
-- Pre-transform stats and filter nulls BEFORE aggregation
file_column_stats_transformed AS (
    SELECT 
        fca.table_id,
        fca.data_file_id,
        fca.column_name,
        fca.column_type,
        fca.value_count,
        fca.null_count,
        -- Transform min_value and check for NULL result
        CASE
            WHEN fca.min_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN 
                regexp_replace(
                    regexp_replace(replace(fca.min_value, ' ', 'T'), '[+-]\\d{2}(?::\\d{2})?$', ''), 
                    '^([^.]+)$', '\\1.000'
                ) || 'Z'
            WHEN contains(lower(fca.column_type), 'date') THEN fca.min_value
            WHEN contains(lower(fca.column_type), 'bool') THEN CAST(lower(fca.min_value) IN ('true', 't', '1', 'yes') AS VARCHAR)
            WHEN contains(lower(fca.column_type), 'int') OR contains(lower(fca.column_type), 'float') 
                 OR contains(lower(fca.column_type), 'double') OR contains(lower(fca.column_type), 'decimal') THEN
                CASE WHEN contains(fca.min_value, '.') OR contains(lower(fca.min_value), 'e') 
                     THEN CAST(TRY_CAST(fca.min_value AS DOUBLE) AS VARCHAR)
                     ELSE CAST(TRY_CAST(fca.min_value AS BIGINT) AS VARCHAR)
                END
            ELSE fca.min_value
        END AS transformed_min,
        -- Transform max_value and check for NULL result
        CASE
            WHEN fca.max_value IS NULL THEN NULL
            WHEN contains(lower(fca.column_type), 'timestamp') THEN 
                regexp_replace(
                    regexp_replace(replace(fca.max_value, ' ', 'T'), '[+-]\\d{2}(?::\\d{2})?$', ''), 
                    '^([^.]+)$', '\\1.000'
                ) || 'Z'
            WHEN contains(lower(fca.column_type), 'date') THEN fca.max_value
            WHEN contains(lower(fca.column_type), 'bool') THEN CAST(lower(fca.max_value) IN ('true', 't', '1', 'yes') AS VARCHAR)
            WHEN contains(lower(fca.column_type), 'int') OR contains(lower(fca.column_type), 'float') 
                 OR contains(lower(fca.column_type), 'double') OR contains(lower(fca.column_type), 'decimal') THEN
                CASE WHEN contains(fca.max_value, '.') OR contains(lower(fca.max_value), 'e') 
                     THEN CAST(TRY_CAST(fca.max_value AS DOUBLE) AS VARCHAR)
                     ELSE CAST(TRY_CAST(fca.max_value AS BIGINT) AS VARCHAR)
                END
            ELSE fca.max_value
        END AS transformed_max
    FROM file_column_stats_agg fca
),
file_metadata AS (
    SELECT 
        ts.table_id,
        ts.schema_name,
        ts.table_name,
        ts.snapshot_id,
        ts.table_root,
        ts.schema_fields,
        df.data_file_id,
        df.path AS file_path,
        df.file_size_bytes,
        COALESCE(MAX(fct.value_count), 0) AS num_records,
        -- Note: Only include columns that have non-null transformed values
        -- Power BI DirectLake requires clean JSON without null values in stats
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.transformed_min
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.transformed_min IS NOT NULL)), MAP{}::MAP(VARCHAR, VARCHAR)) AS min_values,
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.transformed_max
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.transformed_max IS NOT NULL)), MAP{}::MAP(VARCHAR, VARCHAR)) AS max_values,
        COALESCE(map_from_entries(list({
            'key': fct.column_name,
            'value': fct.null_count
        } ORDER BY fct.column_name) FILTER (WHERE fct.column_name IS NOT NULL AND fct.null_count IS NOT NULL)), MAP{}::MAP(VARCHAR, BIGINT)) AS null_count
    FROM table_schemas ts
    JOIN ducklake_data_file df ON ts.table_id = df.table_id
    LEFT JOIN file_column_stats_transformed fct ON df.data_file_id = fct.data_file_id
    WHERE df.begin_snapshot <= ts.snapshot_id
      AND (df.end_snapshot IS NULL OR df.end_snapshot > ts.snapshot_id)
    GROUP BY ts.table_id, ts.schema_name, ts.table_name, ts.snapshot_id, 
             ts.table_root, ts.schema_fields, df.data_file_id, df.path, df.file_size_bytes
),
table_aggregates AS (
    SELECT 
        table_id,
        schema_name,
        table_name,
        snapshot_id,
        table_root,
        schema_fields,
        COUNT(*) AS num_files,
        SUM(num_records) AS total_rows,
        SUM(file_size_bytes) AS total_bytes,
        list({
            'path': ltrim(file_path, '/'),
            'partitionValues': MAP{}::MAP(VARCHAR, VARCHAR),
            'size': file_size_bytes,
            'modificationTime': epoch_ms(now()),
            'dataChange': true,
            -- Ensure stats is always valid JSON - Power BI DirectLake requires non-empty JSON
            'stats': COALESCE(to_json({
                'numRecords': COALESCE(num_records, 0),
                'minValues': COALESCE(min_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                'maxValues': COALESCE(max_values, MAP{}::MAP(VARCHAR, VARCHAR)),
                'nullCount': COALESCE(null_count, MAP{}::MAP(VARCHAR, BIGINT))
            }), '{"numRecords":0}'),
            'tags': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(
            path VARCHAR,
            partitionValues MAP(VARCHAR, VARCHAR),
            size BIGINT,
            modificationTime BIGINT,
            dataChange BOOLEAN,
            stats VARCHAR,
            tags MAP(VARCHAR, VARCHAR)
        )) AS add_entries
    FROM file_metadata
    GROUP BY table_id, schema_name, table_name, snapshot_id, table_root, schema_fields
),
checkpoint_data AS (
    SELECT 
        ta.*,
        now() AS now_ts,
        epoch_ms(now()) AS now_ms,
        uuid()::VARCHAR AS txn_id,
        -- Generate stable UUID based on table_id so it doesn't change across exports
        -- Format as proper UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        (substring(md5(ta.table_id::VARCHAR || '-metadata'), 1, 8) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 9, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 13, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 17, 4) || '-' ||
         substring(md5(ta.table_id::VARCHAR || '-metadata'), 21, 12)) AS meta_id,
        to_json({'type': 'struct', 'fields': ta.schema_fields}) AS schema_string
    FROM table_aggregates ta
),
checkpoint_parquet_data AS (
    SELECT 
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        {'minReaderVersion': 1, 'minWriterVersion': 2} AS protocol,
        NULL AS metaData,
        NULL AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        1 AS row_order
    FROM checkpoint_data cd
    UNION ALL
    SELECT 
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        NULL AS protocol,
        {
            'id': cd.meta_id,
            'name': cd.table_name,
            'format': {'provider': 'parquet', 'options': MAP{}::MAP(VARCHAR, VARCHAR)}::STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            'schemaString': cd.schema_string,
            'partitionColumns': []::VARCHAR[],
            'createdTime': cd.now_ms,
            'configuration': MAP{}::MAP(VARCHAR, VARCHAR)
        }::STRUCT(
            id VARCHAR,
            name VARCHAR,
            format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            schemaString VARCHAR,
            partitionColumns VARCHAR[],
            createdTime BIGINT,
            configuration MAP(VARCHAR, VARCHAR)
        ) AS metaData,
        NULL AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        2 AS row_order
    FROM checkpoint_data cd
    UNION ALL
    SELECT 
        cd.table_id,
        cd.schema_name,
        cd.table_name,
        cd.snapshot_id,
        cd.table_root,
        cd.meta_id,
        cd.now_ms,
        cd.txn_id,
        cd.schema_string,
        cd.num_files,
        cd.total_rows,
        cd.total_bytes,
        NULL AS protocol,
        NULL AS metaData,
        unnest(cd.add_entries) AS add,
        NULL::STRUCT(
            path VARCHAR,
            deletionTimestamp BIGINT,
            dataChange BOOLEAN
        ) AS remove,
        NULL::STRUCT(
            timestamp TIMESTAMP,
            operation VARCHAR,
            operationParameters MAP(VARCHAR, VARCHAR),
            isolationLevel VARCHAR,
            isBlindAppend BOOLEAN,
            operationMetrics MAP(VARCHAR, VARCHAR),
            engineInfo VARCHAR,
            txnId VARCHAR
        ) AS commitInfo,
        3 AS row_order
    FROM checkpoint_data cd
)
SELECT * FROM checkpoint_parquet_data;

-- Create temp_checkpoint_json by deriving from temp_checkpoint_parquet
CREATE OR REPLACE TEMP TABLE temp_checkpoint_json AS 
SELECT DISTINCT
    p.table_id,
    p.table_root,
    p.snapshot_id,
    p.num_files,
    to_json({
        'commitInfo': {
            'timestamp': p.now_ms,
            'operation': 'CONVERT',
            'operationParameters': {
                'convertedFrom': 'DuckLake',
                'duckLakeSnapshotId': p.snapshot_id::VARCHAR,
                'partitionBy': '[]'
            },
            'isolationLevel': 'Serializable',
            'isBlindAppend': false,
            'operationMetrics': {
                'numFiles': p.num_files::VARCHAR,
                'numOutputRows': p.total_rows::VARCHAR,
                'numOutputBytes': p.total_bytes::VARCHAR
            },
            'engineInfo': 'DuckLake-Delta-Exporter/1.0.0',
            'txnId': p.txn_id
        }
    }) || chr(10) ||
    to_json({
        'metaData': {
            'id': p.meta_id,
            'name': p.table_name,
            'format': {'provider': 'parquet', 'options': MAP{}},
            'schemaString': p.schema_string::VARCHAR,
            'partitionColumns': [],
            'createdTime': p.now_ms,
            'configuration': MAP{}
        }
    }) || chr(10) ||
    to_json({
        'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}
    }) AS content
FROM temp_checkpoint_parquet p
WHERE p.row_order = 1;

-- Create last checkpoint temp table
-- Note: Power BI DirectLake requires _last_checkpoint to be valid JSON without null values
-- Only include required fields (version, size) - omit optional fields entirely
CREATE OR REPLACE TEMP TABLE temp_last_checkpoint AS 
SELECT 
    table_id,
    table_root,
    snapshot_id,
    '{"version":' || snapshot_id || ',"size":' || (2 + num_files) || '}' AS content
FROM temp_checkpoint_parquet
GROUP BY table_id, table_root, snapshot_id, num_files;

-- Export checkpoint files for tables that need export
-- Create a numbered list of all tables to export
CREATE OR REPLACE TEMP TABLE numbered_exports AS
SELECT 
    table_id, 
    table_root, 
    snapshot_id,
    ROW_NUMBER() OVER (ORDER BY table_id) AS export_order
FROM (SELECT DISTINCT table_id, table_root, snapshot_id FROM temp_checkpoint_parquet);

-- Generate export tables using filtering on row number
CREATE OR REPLACE TEMP TABLE first_export AS
SELECT table_id, table_root, snapshot_id FROM numbered_exports WHERE export_order = 1;

CREATE OR REPLACE TEMP TABLE second_export AS
SELECT table_id, table_root, snapshot_id FROM numbered_exports WHERE export_order = 2;

CREATE OR REPLACE TEMP TABLE third_export AS
SELECT table_id, table_root, snapshot_id FROM numbered_exports WHERE export_order = 3;

CREATE OR REPLACE TEMP TABLE fourth_export AS
SELECT table_id, table_root, snapshot_id FROM numbered_exports WHERE export_order = 4;

CREATE OR REPLACE TEMP TABLE fifth_export AS
SELECT table_id, table_root, snapshot_id FROM numbered_exports WHERE export_order = 5;

CREATE OR REPLACE TEMP TABLE sixth_export AS
SELECT table_id, table_root, snapshot_id FROM numbered_exports WHERE export_order = 6;

CREATE OR REPLACE TEMP TABLE seventh_export AS
SELECT table_id, table_root, snapshot_id FROM numbered_exports WHERE export_order = 7;

-- Process first export
begin transaction ;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM first_export);
SET VARIABLE checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' FROM first_export LIMIT 1), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' FROM first_export LIMIT 1), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/_last_checkpoint' FROM first_export LIMIT 1), '/tmp/skip');

COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM first_export) AND snapshot_id = (SELECT snapshot_id FROM first_export) ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM first_export) AND snapshot_id = (SELECT snapshot_id FROM first_export)) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM first_export) AND snapshot_id = (SELECT snapshot_id FROM first_export)) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id) SELECT table_id, snapshot_id FROM first_export WHERE getvariable('has_rows') > 0;
commit ;
-- Process second export (if exists)
begin transaction ;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM second_export);
SET VARIABLE checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' FROM second_export LIMIT 1), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' FROM second_export LIMIT 1), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/_last_checkpoint' FROM second_export LIMIT 1), '/tmp/skip');

COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM second_export) AND snapshot_id = (SELECT snapshot_id FROM second_export) ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM second_export) AND snapshot_id = (SELECT snapshot_id FROM second_export)) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM second_export) AND snapshot_id = (SELECT snapshot_id FROM second_export)) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id) SELECT table_id, snapshot_id FROM second_export WHERE getvariable('has_rows') > 0;
commit ;
begin transaction ;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM third_export);
SET VARIABLE checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' FROM third_export LIMIT 1), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' FROM third_export LIMIT 1), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/_last_checkpoint' FROM third_export LIMIT 1), '/tmp/skip');
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM third_export) AND snapshot_id = (SELECT snapshot_id FROM third_export) ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM third_export) AND snapshot_id = (SELECT snapshot_id FROM third_export)) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM third_export) AND snapshot_id = (SELECT snapshot_id FROM third_export)) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id) SELECT table_id, snapshot_id FROM third_export WHERE getvariable('has_rows') > 0;
commit ;
-- Process fourth export (if exists)
begin transaction ;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM fourth_export);
SET VARIABLE checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' FROM fourth_export LIMIT 1), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' FROM fourth_export LIMIT 1), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/_last_checkpoint' FROM fourth_export LIMIT 1), '/tmp/skip');
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM fourth_export) AND snapshot_id = (SELECT snapshot_id FROM fourth_export) ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM fourth_export) AND snapshot_id = (SELECT snapshot_id FROM fourth_export)) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM fourth_export) AND snapshot_id = (SELECT snapshot_id FROM fourth_export)) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id) SELECT table_id, snapshot_id FROM fourth_export WHERE getvariable('has_rows') > 0;
commit ;
-- Process fifth export (if exists)
begin transaction ;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM fifth_export);
SET VARIABLE checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' FROM fifth_export LIMIT 1), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' FROM fifth_export LIMIT 1), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/_last_checkpoint' FROM fifth_export LIMIT 1), '/tmp/skip');
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM fifth_export) AND snapshot_id = (SELECT snapshot_id FROM fifth_export) ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM fifth_export) AND snapshot_id = (SELECT snapshot_id FROM fifth_export)) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM fifth_export) AND snapshot_id = (SELECT snapshot_id FROM fifth_export)) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id) SELECT table_id, snapshot_id FROM fifth_export WHERE getvariable('has_rows') > 0;
commit ;
-- Process sixth export (if exists)
begin transaction ;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM sixth_export);
SET VARIABLE checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' FROM sixth_export LIMIT 1), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' FROM sixth_export LIMIT 1), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/_last_checkpoint' FROM sixth_export LIMIT 1), '/tmp/skip');
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM sixth_export) AND snapshot_id = (SELECT snapshot_id FROM sixth_export) ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM sixth_export) AND snapshot_id = (SELECT snapshot_id FROM sixth_export)) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM sixth_export) AND snapshot_id = (SELECT snapshot_id FROM sixth_export)) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id) SELECT table_id, snapshot_id FROM sixth_export WHERE getvariable('has_rows') > 0;
commit ;
-- Process seventh export (if exists)
begin transaction ;
SET VARIABLE has_rows = (SELECT COUNT(*) FROM seventh_export);
SET VARIABLE checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' FROM seventh_export LIMIT 1), '/tmp/skip');
SET VARIABLE json_file = COALESCE((SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' FROM seventh_export LIMIT 1), '/tmp/skip');
SET VARIABLE last_checkpoint_file = COALESCE((SELECT table_root || '/_delta_log/_last_checkpoint' FROM seventh_export LIMIT 1), '/tmp/skip');
COPY (SELECT protocol, metaData, add, remove, commitInfo FROM temp_checkpoint_parquet WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM seventh_export) AND snapshot_id = (SELECT snapshot_id FROM seventh_export) ORDER BY row_order) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);
COPY (SELECT content FROM temp_checkpoint_json WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM seventh_export) AND snapshot_id = (SELECT snapshot_id FROM seventh_export)) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, QUOTE '');
COPY (SELECT content FROM temp_last_checkpoint WHERE getvariable('has_rows') > 0 AND table_id = (SELECT table_id FROM seventh_export) AND snapshot_id = (SELECT snapshot_id FROM seventh_export)) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, QUOTE '');
INSERT INTO ducklake_export_log (table_id, snapshot_id) SELECT table_id, snapshot_id FROM seventh_export WHERE getvariable('has_rows') > 0;
commit ;

-- Show overall export status
SELECT 
    status,
    COUNT(*) AS table_count,
    list(schema_name || '.' || table_name) AS tables
FROM export_summary
GROUP BY status
ORDER BY 
    CASE status 
        WHEN 'needs_export' THEN 1 
        WHEN 'already_exported' THEN 2 
        WHEN 'no_data_files' THEN 3
        WHEN 'no_snapshots' THEN 4 
    END; 