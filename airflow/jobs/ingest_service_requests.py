from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import os

from logit import get_logger
from pathlib import Path

# Setting up logging variables
dag_name="public_data_pipeline"
script_path = Path(__file__).resolve()
mod_name = script_path.stem

# Set env variables for data locations
ENV_AIRFLOW_LOGS="AIRFLOW_LOGS"
ENV_AIRFLOW_DATA="AIRFLOW_DATA"
data_dir_path = os.getenv(ENV_AIRFLOW_DATA)
log_dir_path = os.getenv(ENV_AIRFLOW_LOGS)
log_dir_path = os.path.join(log_dir_path,dag_name)

# Initiating logger 
logger = get_logger(name=mod_name, log_dir=log_dir_path, log_file=dag_name)
logger.info(f"Starting {mod_name}")

input_file_path = os.path.join(data_dir_path, "raw", "service_requests_raw.csv")
output_file_path = os.path.join(data_dir_path, "bronze", "service_requests")

# Check for existence of files
logger.info(f"Checking for existence of {input_file_path} and {output_file_path}")
for file in (input_file_path,output_file_path):
    if not os.path.exists(file):
        logger.error(f"{file} is missing")

# Create spark session
try:
    spark = SparkSession.builder.appName(mod_name).getOrCreate()
    logger.info(f"Spark session for {mod_name} created")
except Exception as e:
    logger.exception(e)

# Create raw dataframe
try:
    raw_df = spark.read.csv(input_file_path, inferSchema=True, header=True)
    logger.info(f"raw dataframe created")
except Exception as e:
    logger.exception(e)


# Minimal transformations (Bronze)
try:
    bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp())
    logger.info(f"bronze dataframe created")
except Exception as e:
    logger.exception(e)

# Write to parquet file
try:
    bronze_df.write.mode("append").parquet(output_file_path)
    logger.info(f"Parquet file creation: {output_file_path} successful.")
except Exception as e:
    logger.exception(e)
