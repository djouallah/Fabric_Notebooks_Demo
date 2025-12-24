
ATTACH or replace 'new.db' AS dwh_export ; 
USE dwh_export ;

-- Create export tracking table if it doesn't exist
CREATE TABLE IF NOT EXISTS ducklake_export_log (
    table_id BIGINT,
    snapshot_id BIGINT,
    export_timestamp TIMESTAMP DEFAULT now(),
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
        (SELECT MAX(begin_snapshot) 
         FROM ducklake_data_file 
         WHERE table_id = t.table_id) AS latest_snapshot
    FROM active_tables t
),
export_status AS (
    SELECT 
        ls.*,
        el.snapshot_id AS exported_snapshot
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
        WHEN exported_snapshot IS NOT NULL THEN 'already_exported'
        ELSE 'needs_export'
    END AS status,
    latest_snapshot,
    CASE 
        WHEN latest_snapshot IS NULL THEN 'No snapshots found'
        WHEN exported_snapshot IS NOT NULL THEN 'Snapshot ' || latest_snapshot || ' already exported'
        ELSE 'Ready for export'
    END AS message
FROM export_status;

-- 4. Generate checkpoint files for tables that need export  
CREATE OR REPLACE TEMP TABLE temp_checkpoint_parquet AS
WITH 
tables_to_export AS (
    SELECT * FROM export_summary WHERE status = 'needs_export'
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
        c.column_type,
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
    GROUP BY ts.table_id, df.data_file_id, c.column_name, c.column_type
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
        COALESCE(MAX(fca.value_count), 0) AS num_records,
        COALESCE(map_from_entries(list({
            'key': fca.column_name,
            'value': 
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
                END
        })), MAP{}) AS min_values,
        COALESCE(map_from_entries(list({
            'key': fca.column_name,
            'value': 
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
                END
        })), MAP{}) AS max_values,
        COALESCE(map_from_entries(list({
            'key': fca.column_name,
            'value': fca.null_count
        })), MAP{}) AS null_count
    FROM table_schemas ts
    JOIN ducklake_data_file df ON ts.table_id = df.table_id
    LEFT JOIN file_column_stats_agg fca ON df.data_file_id = fca.data_file_id
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
            'stats': to_json({
                'numRecords': num_records,
                'minValues': min_values,
                'maxValues': max_values,
                'nullCount': null_count
            }),
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
        epoch_ms(now()) AS now_ms,
        uuid()::VARCHAR AS txn_id,
        uuid()::VARCHAR AS meta_id,
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
        NULL AS remove,
        NULL AS commitInfo,
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
            'description': NULL,
            'format': {'provider': 'parquet', 'options': MAP{}::MAP(VARCHAR, VARCHAR)}::STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            'schemaString': cd.schema_string,
            'partitionColumns': []::VARCHAR[],
            'createdTime': cd.now_ms,
            'configuration': MAP{'delta.logRetentionDuration': 'interval 1 hour'}
        }::STRUCT(
            id VARCHAR,
            name VARCHAR,
            description VARCHAR,
            format STRUCT(provider VARCHAR, options MAP(VARCHAR, VARCHAR)),
            schemaString VARCHAR,
            partitionColumns VARCHAR[],
            createdTime BIGINT,
            configuration MAP(VARCHAR, VARCHAR)
        ) AS metaData,
        NULL AS add,
        NULL AS remove,
        NULL AS commitInfo,
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
        NULL AS remove,
        NULL AS commitInfo,
        3 AS row_order
    FROM checkpoint_data cd
),
checkpoint_json_data AS (
    SELECT 
        cd.table_id,
        cd.table_root,
        cd.snapshot_id,
        cd.num_files,
        to_json({
            'commitInfo': {
                'timestamp': cd.now_ms,
                'operation': 'CONVERT',
                'operationParameters': {
                    'convertedFrom': 'DuckLake',
                    'duckLakeSnapshotId': cd.snapshot_id::VARCHAR,
                    'partitionBy': '[]'
                },
                'isolationLevel': 'Serializable',
                'isBlindAppend': false,
                'operationMetrics': {
                    'numFiles': cd.num_files::VARCHAR,
                    'numOutputRows': cd.total_rows::VARCHAR,
                    'numOutputBytes': cd.total_bytes::VARCHAR
                },
                'engineInfo': 'DuckLake-Delta-Exporter/1.0.0',
                'txnId': cd.txn_id
            }
        }) || chr(10) ||
        to_json({
            'metaData': {
                'id': cd.meta_id,
                'name': cd.table_name,
                'description': NULL,
                'format': {'provider': 'parquet', 'options': MAP{}},
                'schemaString': cd.schema_string::VARCHAR,
                'partitionColumns': [],
                'createdTime': cd.now_ms,
                'configuration': MAP{}
            }
        }) || chr(10) ||
        to_json({
            'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}
        }) || chr(10) AS content
    FROM checkpoint_data cd
),
last_checkpoint_data AS (
    SELECT 
        cd.table_id,
        cd.table_root,
        cd.snapshot_id,
        to_json({
            'version': cd.snapshot_id,
            'size': 2 + cd.num_files
        }) AS content
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
            'description': NULL,
            'format': {'provider': 'parquet', 'options': MAP{}},
            'schemaString': p.schema_string::VARCHAR,
            'partitionColumns': [],
            'createdTime': p.now_ms,
            'configuration': MAP{}
        }
    }) || chr(10) ||
    to_json({
        'protocol': {'minReaderVersion': 1, 'minWriterVersion': 2}
    }) || chr(10) AS content
FROM temp_checkpoint_parquet p
WHERE p.row_order = 1;

-- Create last checkpoint temp table
CREATE OR REPLACE TEMP TABLE temp_last_checkpoint AS 
SELECT 
    table_id,
    table_root,
    snapshot_id,
    to_json({
        'version': snapshot_id,
        'size': 2 + num_files
    }) AS content
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

-- Process first export

-- Set file paths as variables
SET VARIABLE checkpoint_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' 
    FROM first_export
);

SET VARIABLE json_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' 
    FROM first_export
);

SET VARIABLE last_checkpoint_file = (
    SELECT table_root || '/_delta_log/_last_checkpoint' 
    FROM first_export
);

-- Export checkpoint parquet file
COPY (
    SELECT protocol, metaData, add, remove, commitInfo 
    FROM temp_checkpoint_parquet 
    WHERE table_id = (SELECT table_id FROM first_export)
      AND snapshot_id = (SELECT snapshot_id FROM first_export)
    ORDER BY row_order
) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);

-- Export JSON transaction log
COPY (
    SELECT content 
    FROM temp_checkpoint_json 
    WHERE table_id = (SELECT table_id FROM first_export)
      AND snapshot_id = (SELECT snapshot_id FROM first_export)
) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

-- Export last checkpoint metadata
COPY (
    SELECT content 
    FROM temp_last_checkpoint 
    WHERE table_id = (SELECT table_id FROM first_export)
      AND snapshot_id = (SELECT snapshot_id FROM first_export)
) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

-- Log the exported snapshot
INSERT INTO ducklake_export_log (table_id, snapshot_id)
SELECT table_id, snapshot_id FROM first_export;

-- Show exported table details
SELECT 
    p.table_name,
    p.table_root,
    p.snapshot_id
FROM temp_checkpoint_parquet p
WHERE p.table_id = (SELECT table_id FROM first_export)
  AND p.snapshot_id = (SELECT snapshot_id FROM first_export)
LIMIT 1;

-- Process second export (if exists)

SET VARIABLE checkpoint_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' 
    FROM second_export
);

SET VARIABLE json_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' 
    FROM second_export
);

SET VARIABLE last_checkpoint_file = (
    SELECT table_root || '/_delta_log/_last_checkpoint' 
    FROM second_export
);

COPY (
    SELECT protocol, metaData, add, remove, commitInfo 
    FROM temp_checkpoint_parquet 
    WHERE table_id = (SELECT table_id FROM second_export)
      AND snapshot_id = (SELECT snapshot_id FROM second_export)
    ORDER BY row_order
) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);

COPY (
    SELECT content 
    FROM temp_checkpoint_json 
    WHERE table_id = (SELECT table_id FROM second_export)
      AND snapshot_id = (SELECT snapshot_id FROM second_export)
) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

COPY (
    SELECT content 
    FROM temp_last_checkpoint 
    WHERE table_id = (SELECT table_id FROM second_export)
      AND snapshot_id = (SELECT snapshot_id FROM second_export)
) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

INSERT INTO ducklake_export_log (table_id, snapshot_id)
SELECT table_id, snapshot_id FROM second_export;

SELECT 
    p.table_name,
    p.table_root,
    p.snapshot_id
FROM temp_checkpoint_parquet p
WHERE p.table_id = (SELECT table_id FROM second_export)
  AND p.snapshot_id = (SELECT snapshot_id FROM second_export)
LIMIT 1;

-- Process third export (if exists)

SET VARIABLE checkpoint_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' 
    FROM third_export
);

SET VARIABLE json_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' 
    FROM third_export
);

SET VARIABLE last_checkpoint_file = (
    SELECT table_root || '/_delta_log/_last_checkpoint' 
    FROM third_export
);

COPY (
    SELECT protocol, metaData, add, remove, commitInfo 
    FROM temp_checkpoint_parquet 
    WHERE table_id = (SELECT table_id FROM third_export)
      AND snapshot_id = (SELECT snapshot_id FROM third_export)
    ORDER BY row_order
) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);

COPY (
    SELECT content 
    FROM temp_checkpoint_json 
    WHERE table_id = (SELECT table_id FROM third_export)
      AND snapshot_id = (SELECT snapshot_id FROM third_export)
) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

COPY (
    SELECT content 
    FROM temp_last_checkpoint 
    WHERE table_id = (SELECT table_id FROM third_export)
      AND snapshot_id = (SELECT snapshot_id FROM third_export)
) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

INSERT INTO ducklake_export_log (table_id, snapshot_id)
SELECT table_id, snapshot_id FROM third_export;

SELECT 
    p.table_name,
    p.table_root,
    p.snapshot_id
FROM temp_checkpoint_parquet p
WHERE p.table_id = (SELECT table_id FROM third_export)
  AND p.snapshot_id = (SELECT snapshot_id FROM third_export)
LIMIT 1;

-- Process fourth export (if exists)

SET VARIABLE checkpoint_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' 
    FROM fourth_export
);

SET VARIABLE json_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' 
    FROM fourth_export
);

SET VARIABLE last_checkpoint_file = (
    SELECT table_root || '/_delta_log/_last_checkpoint' 
    FROM fourth_export
);

COPY (
    SELECT protocol, metaData, add, remove, commitInfo 
    FROM temp_checkpoint_parquet 
    WHERE table_id = (SELECT table_id FROM fourth_export)
      AND snapshot_id = (SELECT snapshot_id FROM fourth_export)
    ORDER BY row_order
) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);

COPY (
    SELECT content 
    FROM temp_checkpoint_json 
    WHERE table_id = (SELECT table_id FROM fourth_export)
      AND snapshot_id = (SELECT snapshot_id FROM fourth_export)
) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

COPY (
    SELECT content 
    FROM temp_last_checkpoint 
    WHERE table_id = (SELECT table_id FROM fourth_export)
      AND snapshot_id = (SELECT snapshot_id FROM fourth_export)
) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

INSERT INTO ducklake_export_log (table_id, snapshot_id)
SELECT table_id, snapshot_id FROM fourth_export;

SELECT 
    p.table_name,
    p.table_root,
    p.snapshot_id
FROM temp_checkpoint_parquet p
WHERE p.table_id = (SELECT table_id FROM fourth_export)
  AND p.snapshot_id = (SELECT snapshot_id FROM fourth_export)
LIMIT 1;

-- Process fifth export (if exists)

SET VARIABLE checkpoint_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.checkpoint.parquet' 
    FROM fifth_export
);

SET VARIABLE json_file = (
    SELECT table_root || '/_delta_log/' || lpad(snapshot_id::VARCHAR, 20, '0') || '.json' 
    FROM fifth_export
);

SET VARIABLE last_checkpoint_file = (
    SELECT table_root || '/_delta_log/_last_checkpoint' 
    FROM fifth_export
);

COPY (
    SELECT protocol, metaData, add, remove, commitInfo 
    FROM temp_checkpoint_parquet 
    WHERE table_id = (SELECT table_id FROM fifth_export)
      AND snapshot_id = (SELECT snapshot_id FROM fifth_export)
    ORDER BY row_order
) TO (getvariable('checkpoint_file')) (FORMAT PARQUET);

COPY (
    SELECT content 
    FROM temp_checkpoint_json 
    WHERE table_id = (SELECT table_id FROM fifth_export)
      AND snapshot_id = (SELECT snapshot_id FROM fifth_export)
) TO (getvariable('json_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

COPY (
    SELECT content 
    FROM temp_last_checkpoint 
    WHERE table_id = (SELECT table_id FROM fifth_export)
      AND snapshot_id = (SELECT snapshot_id FROM fifth_export)
) TO (getvariable('last_checkpoint_file')) (FORMAT CSV, HEADER false, DELIMITER '', QUOTE '');

INSERT INTO ducklake_export_log (table_id, snapshot_id)
SELECT table_id, snapshot_id FROM fifth_export;

SELECT 
    p.table_name,
    p.table_root,
    p.snapshot_id
FROM temp_checkpoint_parquet p
WHERE p.table_id = (SELECT table_id FROM fifth_export)
  AND p.snapshot_id = (SELECT snapshot_id FROM fifth_export)
LIMIT 1;