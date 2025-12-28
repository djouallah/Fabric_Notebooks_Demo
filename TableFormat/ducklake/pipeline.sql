CREATE or replace  SECRET secret_azure (TYPE azure, PROVIDER credential_chain, CHAIN 'cli', ACCOUNT_NAME 'onelake');
CREATE or replace SECRET my_secret ( TYPE ducklake,
    METADATA_PATH 'sqlite:metadata.db',
    DATA_PATH 'abfss://ducklake@onelake.dfs.fabric.microsoft.com/data.Lakehouse/Tables');
ATTACH or replace 'ducklake:my_secret' AS dwh ; USE dwh ;
create schema  if not exists bronze;use bronze; 
CALL set_option('parquet_row_group_size', 2048*2000) ;
CALL set_option('target_file_size', '128MB') ;
CALL set_option('parquet_compression', 'ZSTD');
CALL set_option('parquet_version', 1);
CALL set_option('rewrite_delete_threshold', 0);
.read process.sql
.read intraday.sql
.read export.sql