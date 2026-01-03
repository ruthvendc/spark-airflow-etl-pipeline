import os
from pathlib import Path

# Return mod name
def get_mod_name(script_file):
    script_path = Path(script_file).resolve()
    return script_path.stem


def get_dag_name():
    dag_name = os.getenv("DAG_NAME")
    if not dag_name:
        raise EnvironmentError("DAG_NAME not set")
    return dag_name


def get_data_dir():
    return os.getenv("AIRFLOW_DATA")


def get_log_dir(dag_name):
    base_log_dir = os.getenv("AIRFLOW_LOGS")
    if not base_log_dir:
        raise EnvironmentError("AIRFLOW_LOGS is not set")
    return os.path.join(base_log_dir, dag_name)
