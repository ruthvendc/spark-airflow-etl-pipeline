from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os

# Set env variables for data locations
ENV_AIRFLOW_DATA="AIRFLOW_DATA"
directory_path = os.getenv(ENV_AIRFLOW_DATA)

input_file_path = os.path.join(directory_path, "raw", "facilities_raw.csv")
output_file_path = os.path.join(directory_path, "bronze", "facilities")

spark = SparkSession.builder.appName("IngestFacilities").getOrCreate()

raw_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(input_file_path)
)

# Minimal transformations (Bronze)
bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp())

# Write to parquet file
bronze_df.write.mode("append").parquet(output_file_path)
