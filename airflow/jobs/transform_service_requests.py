from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

# Set env variables for data locations
ENV_AIRFLOW_DATA="AIRFLOW_DATA"
directory_path = os.getenv(ENV_AIRFLOW_DATA)

input_file_path = os.path.join(directory_path, "bronze", "service_requests")
output_file_path = os.path.join(directory_path, "silver", "service_requests")

spark = SparkSession.builder.appName("TransformServiceRequests").getOrCreate()

# Modifies null values in column "resolution_time_hr" and drops duplicate "request_id"
silver_df = (
    spark.read.parquet(input_file_path)
         .filter(col("facility_id").isNotNull())
         .withColumn(
             "resolution_time_hr",
             when(col("resolution_time_hr").isNull(), None)
             .otherwise(col("resolution_time_hr"))
         )
         .dropDuplicates(["request_id"])
     )

# Write to silver location
silver_df.write.mode("overwrite").parquet(output_file_path)
