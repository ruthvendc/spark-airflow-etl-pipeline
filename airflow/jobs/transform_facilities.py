from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
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


input_file_path = os.path.join(data_dir_path, "bronze", "facilities")
output_file_path = os.path.join(data_dir_path, "silver", "facilities")


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


# Modifies null values in column "resolution_time_hr" and drops duplicate "request_id"
try:
    silver_df = (
        spark.read.parquet(input_file_path)
        .dropDuplicates(["facility_id"])
        .filter(col("facility_id").isNotNull())
        .withColumn(
            "status",
            when(col("status").isNull(), "UNKNOWN")
            .otherwise(col("status"))
        )
    )
    logger.info(f"silver dataframe created")
except Exception as e:
    logger.exception(e)


# Write to silver location
try:
    silver_df.write.mode("overwrite").parquet(output_file_path)
    logger.info(f"Parquet file creation: {output_file_path} successful.")
except Exception as e:
    logger.exception(e)

