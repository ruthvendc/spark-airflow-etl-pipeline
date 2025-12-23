from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, current_date, lit, when, col
import os

# Set env variables for data locations
ENV_AIRFLOW_DATA="AIRFLOW_DATA"
directory_path = os.getenv(ENV_AIRFLOW_DATA)

input_file_path = os.path.join(directory_path, "silver", "facilities")
output_file_path = os.path.join(directory_path, "gold", "dim_facilities")

spark = SparkSession.builder.appName("DimServiceRequests").getOrCreate()

# Add some special columns
dim_facilities = (
    spark.read.parquet(input_file_path)
    .withColumn("facility_sk",monotonically_increasing_id())
    .withColumn("insert_dt",current_date())
    .withColumn("end_date", lit(None).cast("date"))
    .withColumn("is_current", lit(True))
    .withColumn("size",
    when(col("square_feet") < 80000, "Small")
    .when((col("square_feet").cast("integer") > 80000) & (col("square_feet").cast("integer") < 160000), "Medium")
    .when((col("square_feet").cast("integer") > 160000) & (col("square_feet").cast("integer") < 260000), "Large")
    .otherwise("UNKNOWN")
           )
)

# Write to gold location
dim_facilities.write.mode("overwrite").parquet(output_file_path)
