from pyspark.sql import SparkSession
import os 

# Set env variables for data locations
ENV_AIRFLOW_DATA="AIRFLOW_DATA"
directory_path = os.getenv(ENV_AIRFLOW_DATA)

svc_requests_data = os.path.join(directory_path, "silver", "service_requests")
facilities_data = os.path.join(directory_path, "gold", "dim_facilities")

output_file_path = os.path.join(directory_path, "gold", "fact_service_requests")

spark = SparkSession.builder.appName("FactSVCRequest").getOrCreate()

df_svc_requests = spark.read.parquet(svc_requests_data)
df_facilities = spark.read.parquet(facilities_data)

df_svc_requests.createOrReplaceTempView("svc_requests")
df_facilities.createOrReplaceTempView("facilities")

sql = """
select s.request_type, s.priority, cast(s.created_date as date), f.facility_name, f.facility_type, f.size
from svc_requests s join facilities f
on s.facility_id = f.facility_id
"""

fact_requests = spark.sql(sql)

# Write to gold location
fact_requests.write.partitionBy("created_date").mode("overwrite").parquet(output_file_path)
