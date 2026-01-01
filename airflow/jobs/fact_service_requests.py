from pyspark.sql import SparkSession
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

svc_requests_data = os.path.join(data_dir_path, "silver", "service_requests")
facilities_data = os.path.join(data_dir_path, "gold", "dim_facilities")
output_file_path = os.path.join(data_dir_path, "gold", "fact_service_requests")


# Check for existence of files
logger.info(f"Checking for existence files: {svc_requests_data}, {facilities_data}, {output_file_path}")
for file in (svc_requests_data,facilities_data,output_file_path):
    if not os.path.exists(file):
        logger.error(f"{file} is missing")


# Create spark session
try:
    spark = SparkSession.builder.appName(mod_name).getOrCreate()
    logger.info(f"Spark session for {mod_name} created")
except Exception as e:
    logger.exception(e)


# Create dataframes 
try:
    df_svc_requests = spark.read.parquet(svc_requests_data)
    logger.info(f"Dataframe df_svc_requests created")
except Exception as e:
    logger.exception(e)

try:
    df_facilities = spark.read.parquet(facilities_data)
    logger.info(f"Dataframe df_facilities created")
except Exception as e:
    logger.exception(e)


# Create temp views from dataframes
try:
    df_svc_requests.createOrReplaceTempView("svc_requests")
    logger.info(f"TempView for svc_requests created")
except Exception as e:
    logger.exception(e)

try:
    df_facilities.createOrReplaceTempView("facilities")
    logger.info(f"TempView for facilities created")
except Exception as e:
    logger.exception(e)


# SQL query to be used
sql = """
select s.request_type, s.priority, cast(s.created_date as date), f.facility_name, f.facility_type, f.size
from svc_requests s join facilities f
on s.facility_id = f.facility_id
"""

# Create dataframe from SQL 
try:
    fact_requests = spark.sql(sql)
    logger.info(f"Dataframe from SQL created")
except Exception as e:
    logger.exception(e)


# Write to gold location
try:
    fact_requests.write.partitionBy("created_date").mode("overwrite").parquet(output_file_path)
    logger.info(f"Parquet file creation: {output_file_path} successful.")
except Exception as e:
    logger.exception(e)
