# DATA-INGESTION

End-to-End PySpark Code for Transformation and Loading:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("EndToEndETL").getOrCreate()

# Ingest Data from ADLS
df_raw = spark.read.format("parquet").load("abfss://<container>@<storage-account>.dfs.core.windows.net/<folder>/raw_data/")

# Perform transformations
df_transformed = df_raw.filter(col("sales") > 1000).groupBy("region").agg({"sales": "sum"}).withColumnRenamed("sum(sales)", "total_sales")

# Write transformed data back to ADLS
df_transformed.write.partitionBy("region").format("parquet").save("abfss://<container>@<storage-account>.dfs.core.windows.net/<folder>/transformed_data/")
