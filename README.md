# Light_ETL_Challenge
extract data fom a csv with different number of columns and export to Delta Lake
I started with Duckdb and Polars, but I expect more engines like chdb, Raft etc

the script will download 60 files, around 4 GB uncompressed

Please keep the local Path as 

for raw data : raw_landing='/lakehouse/default/Files/raw'

for Delta : '/lakehouse/default/Tables/'
