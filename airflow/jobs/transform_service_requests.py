from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import os
from logit import get_logger
from pathlib import Path
from config.envsetup import get_mod_name, get_dag_name, get_data_dir, get_log_dir


# Setup env
mod_name = get_mod_name(__file__)
dag_name = get_dag_name()
data_dir_path = get_data_dir()
log_dir_path = get_log_dir(dag_name)


# Initiating logger 
logger = get_logger(name=mod_name, log_dir=log_dir_path, log_file=dag_name)
logger.info(f"Starting {mod_name}")


input_file_path = os.path.join(data_dir_path, "bronze", "service_requests")
output_file_path = os.path.join(data_dir_path, "silver", "service_requests")


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
            .dropDuplicates(["request_id"])
            .filter(col("facility_id").isNotNull())
            .withColumn(
                "resolution_time_hr",
                 when(col("resolution_time_hr").isNull(), 0)
                 .otherwise(col("resolution_time_hr"))
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
