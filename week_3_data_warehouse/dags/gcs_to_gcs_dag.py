import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

taxi_type = ['yellow', 'fhv']

EXEC_DATE = "{{ execution_date.strftime('%Y-%m') }}"
EXEC_YEAR = "{{ execution_date.strftime('%Y') }}"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1,1),
    'end_date': datetime(2020, 12, 1),
    'depends_on_past': False
}

dag = DAG(
    dag_id="gcs_to_gcs_dag",
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=3
)

with dag:
        
    #     move_files_gcs_task = GCSToGCSOperator(
    #     task_id=f"move_{type}_files_gcs_task",
    #     source_bucket=BUCKET,
    #     source_object="raw/{}*.parquet".format(type, EXEC_DATE),
    #     destination_bucket=BUCKET,
    #     destination_object="{}/{}/".format(type, EXEC_YEAR)
    # )

    move_files_gcs_task = GCSToGCSOperator(
        task_id="move_yellow_files_gcs_task",
        source_bucket=BUCKET,
        source_object="raw/yellow_taxi_{}.parquet".format(EXEC_DATE),
        destination_bucket=BUCKET,
        destination_object="yellow/{}/yellow_taxi_{}.parquet".format(EXEC_YEAR, EXEC_DATE)
    )

    move_files_gcs_task