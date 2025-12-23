from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, current_date, lit
import os

# Set env variables for data locations
ENV_AIRFLOW_DATA="AIRFLOW_DATA"
directory_path = os.getenv(ENV_AIRFLOW_DATA)

input_file_path = os.path.join(directory_path, "silver", "service_requests")
output_file_path = os.path.join(directory_path, "gold", "dim_service_requests")

spark = SparkSession.builder.appName("DimServiceRequests").getOrCreate()

# Modifies null values in column "resolution_time_hr" and drops duplicate "request_id"
dim_service_requests = (
    spark.read.parquet(input_file_path)
    .withColumn("facility_sk",monotonically_increasing_id())
    .withColumn("insert_dt",current_date())
    .withColumn("end_date", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
)

# Write to gold location
dim_service_requests.write.mode("overwrite").parquet(output_file_path)
