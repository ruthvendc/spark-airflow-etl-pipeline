from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG Arguments
default_args = {
    'owner': 'David',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="public_data_pipeline",
    start_date=datetime(2025, 11, 13),
    schedule="@daily",
    catchup=False,
) as dag:

    # Define the task named extract_data from service requests raw data
    ingest_svc_requests = BashOperator(
        task_id="ingest_svc_requests",
        bash_command="export DAG_NAME=\"{{ dag.dag_id }}\" && spark-submit --py-files $AIRFLOW_ENV/config.zip $AIRFLOW_JOBS/ingest_service_requests.py"
    )
    ingest_facilities = BashOperator(
        task_id="ingest_facilities",
        bash_command="export DAG_NAME=\"{{ dag.dag_id }}\" && spark-submit --py-files $AIRFLOW_ENV/config.zip $AIRFLOW_JOBS/ingest_facilities.py"
    )
    ingest_maint_costs = BashOperator(
        task_id="ingest_maint_costs",
        bash_command="export DAG_NAME=\"{{ dag.dag_id }}\" && spark-submit --py-files $AIRFLOW_ENV/config.zip $AIRFLOW_JOBS/ingest_maint_costs.py"
    )
    transform_svc_requests = BashOperator(
        task_id="transform_svc_requests",
        bash_command="export DAG_NAME=\"{{ dag.dag_id }}\" && spark-submit --py-files $AIRFLOW_ENV/config.zip $AIRFLOW_JOBS/transform_service_requests.py"
    )
    transform_facilities = BashOperator(
        task_id="transform_facilities",
        bash_command="export DAG_NAME=\"{{ dag.dag_id }}\" && spark-submit --py-files $AIRFLOW_ENV/config.zip $AIRFLOW_JOBS/transform_facilities.py"
    )
    dim_svc_requests = BashOperator(
        task_id="dim_svc_requests",
        bash_command="export DAG_NAME=\"{{ dag.dag_id }}\" && spark-submit --py-files $AIRFLOW_ENV/config.zip $AIRFLOW_JOBS/dim_service_requests.py"
    )
    dim_facilities = BashOperator(
        task_id="dim_facilities",
        bash_command="export DAG_NAME=\"{{ dag.dag_id }}\" && spark-submit --py-files $AIRFLOW_ENV/config.zip $AIRFLOW_JOBS/dim_facilities.py"
    )
    fact_facilities = BashOperator(
        task_id="fact_facilities",
        bash_command="export DAG_NAME=\"{{ dag.dag_id }}\" && spark-submit --py-files $AIRFLOW_ENV/config.zip $AIRFLOW_JOBS/fact_service_requests.py"
    )


    # Define the task pipeline
    ingest_svc_requests >> ingest_facilities >> ingest_maint_costs >> transform_svc_requests >> transform_facilities >> dim_svc_requests >> dim_facilities >> fact_facilities
