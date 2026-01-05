
---- define variables with defaults if not set in environment
SET VARIABLE METADATA_PATH    = COALESCE(NULLIF(getenv('DUCKLAKE_METADATA_PATH'), ''), 'sqlite:/lakehouse/default/Files/sql/metadata.db');
SET VARIABLE download_limit   = COALESCE(TRY_CAST(NULLIF(getenv('DUCKLAKE_DOWNLOAD_LIMIT'), '') AS INTEGER), 288);
SET VARIABLE row_limit        = COALESCE(TRY_CAST(NULLIF(getenv('DUCKLAKE_PROCESS_LIMIT'), '') AS INTEGER), 288);
SET VARIABLE PATH_ROOT        = COALESCE(NULLIF(getenv('DUCKLAKE_DATA_PATH'), ''), 'abfss://data@icebergmainstorage.dfs.core.windows.net/ducklake');
--------------------------------
.bail off
LOAD zipfs;
load azure ;
SET force_download = true;

---- or use service principal if you have one
--CREATE or replace PERSISTENT secret onelake (TYPE azure, PROVIDER credential_chain, CHAIN 'cli', ACCOUNT_NAME 'onelake');
--CREATE or replace PERSISTENT secret azure (TYPE azure, PROVIDER credential_chain, CHAIN 'cli', ACCOUNT_NAME 'icebergmainstorage');

CREATE or replace SECRET my_secret
 ( TYPE ducklake,
    METADATA_PATH  getvariable('METADATA_PATH') ,
    DATA_PATH      getvariable('PATH_ROOT') || '/Tables'
 );
ATTACH or replace 'ducklake:my_secret' AS dwh (DATA_INLINING_ROW_LIMIT 10);
USE dwh ;

CREATE SCHEMA IF NOT EXISTS bronze;
USE bronze;

CALL set_option('parquet_row_group_size', 2048*2000) ;
CALL set_option('target_file_size', '128MB') ;
CALL set_option('parquet_compression', 'ZSTD');
CALL set_option('parquet_version', 1);
CALL set_option('rewrite_delete_threshold', 0);
CALL set_option('expire_older_than', '2 days');
call set_option('delete_older_than', '2 days');


