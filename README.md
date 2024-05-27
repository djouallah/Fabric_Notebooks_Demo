# Light_ETL_Challenge
extract data fom a csv. the number of columns is higher than what's in the header, filter a subset of data and export to Delta Lake
I started with Duckdb , Polars ,Pandas,Pyspark, Pyarrow, Ibis but I expect more engines like chdb, Raft etc

the script will download 60 files, around 4 GB uncompressed

Please keep the local Path as 

for raw data : raw_landing='/lakehouse/default/Files/raw'

for Delta Python : '/lakehouse/default/Tables/'

For Fabric, it will automatically detect Spark

<img width="776" alt="image" src="https://github.com/djouallah/Light_ETL_Challenge/assets/12554469/29ccf459-f29e-47e9-8f7c-0f371fcc5f67">


