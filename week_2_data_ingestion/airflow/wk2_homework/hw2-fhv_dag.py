import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = "https://nyc-tlc.s3.amazonaws.com/trip+data"
URL_TEMPLATE = URL_PREFIX + "/fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"

OUTPUT_FILENAME = 'fhv_{{ execution_date.strftime(\'%Y-%m\') }}'
CSV_OUTPUT_FILE = f'{OUTPUT_FILENAME}.csv'
PQ_OUTPUT_FILE = f'{OUTPUT_FILENAME}.parquet'
CSV_OUTPUT_FILE_TEMPLATE = f'{AIRFLOW_HOME}/{CSV_OUTPUT_FILE}'
PQ_OUTPUT_FILE_TEMPLATE=f'{AIRFLOW_HOME}/{PQ_OUTPUT_FILE}'
# TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}'

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2019, 12,1),
    'depends_on_past': False
}

dag = DAG(
    dag_id="HW2-FHV_local_dag",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=True,
    max_active_runs=3
    )

with dag:
    download_csv_task = BashOperator(
        task_id='download_csv_task',
        bash_command=f'curl -sSLf {URL_TEMPLATE} > {CSV_OUTPUT_FILE_TEMPLATE}'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": CSV_OUTPUT_FILE_TEMPLATE
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{PQ_OUTPUT_FILE}",
            "local_file": f"{AIRFLOW_HOME}/{PQ_OUTPUT_FILE}",
        }
    )

    remove_local_files_task = BashOperator(
        task_id="remove_local_files_task",
        bash_command=f'rm {CSV_OUTPUT_FILE_TEMPLATE} {PQ_OUTPUT_FILE_TEMPLATE}'
    )

    download_csv_task >> format_to_parquet_task >> local_to_gcs_task >> remove_local_files_task