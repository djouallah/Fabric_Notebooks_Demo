set variable endpoint = 'https://onelake.table.fabric.microsoft.com/iceberg'
-- Read and clean token, store in variable
SET variable tokeng = (
  SELECT
    TRIM(REPLACE(REPLACE(REPLACE(content, CHR(13), ''), CHR(10), ''), ' ', ''))
  FROM
    read_text('/lakehouse/default/Files/token.txt')
);

-- Load httpfs extension
LOAD httpfs;

-- Create secret using the variable
CREATE OR REPLACE SECRET onelake__ (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN getvariable ('tokeng'));

-- it works with warehouse or lakehouse too, read only for now
ATTACH or replace 'workspace_name/warehouse_name.warehouse' AS simple ( TYPE ICEBERG, ENDPOINT getvariable('endpoint'),  token getvariable('tokeng') )                        
