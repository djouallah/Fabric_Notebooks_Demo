# Light_ETL_Challenge
extract data fom a csv. the number of columns is higher than what's in the header, filter a subset of data and export to Delta Lake
I started with Duckdb , Polars and Pandas, but I expect more engines like chdb, Raft etc

the script will download 60 files, around 4 GB uncompressed

Please keep the local Path as 

for raw data : raw_landing='/lakehouse/default/Files/raw'

for Delta Python : '/lakehouse/default/Tables/'

When using Spark

for Delta Spark 'Tables/'


<img width="723" alt="image" src="https://github.com/djouallah/Light_ETL_Challenge/assets/12554469/af5c533e-d193-4000-9fb4-bad08efa747d">

