from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp, current_date, lit, when, col
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


input_file_path = os.path.join(data_dir_path, "silver", "facilities")
output_file_path = os.path.join(data_dir_path, "gold", "dim_facilities")


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


# Add some special columns
try:
    # Add some special columns
    dim_facilities = (
        spark.read.parquet(input_file_path)
        .withColumn("facility_sk", monotonically_increasing_id())
        .withColumn("insert_dt", current_timestamp())
        .withColumn("end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("size",
                    when(col("square_feet") < 80000, "SMALL")
                    .when((col("square_feet") > 80000) & (col("square_feet") < 160000), "MEDIUM")
                    .when((col("square_feet") > 160000) & (col("square_feet") < 280000), "LARGE")
                    .otherwise("UNKNOWN")
                    )
    )
    logger.info(f"dim_facilities dataframe created")
except Exception as e:
    logger.exception(e)


# Write to gold location
try:
    dim_facilities.write.mode("overwrite").parquet(output_file_path)
    logger.info(f"Parquet file creation: {output_file_path} successful.")
except Exception as e:
    logger.exception(e)

