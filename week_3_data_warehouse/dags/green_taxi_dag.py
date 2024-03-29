from distutils.command.upload import upload
import os
import logging

from datetime import datetime

# import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as ppq

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime("%Y-%m") }}.csv'
OUTPUT_CSV_FILE = AIRFLOW_HOME + '/output_{{ execution_date.strftime("%Y-%m") }}.csv'
OUTPUT_PQ_FILE = AIRFLOW_HOME + '/output_{{ execution_date.strftime("%Y-%m") }}.parquet'

EXEC_YEAR = '{{ execution_date.strftime("%Y") }}'
EXEC_DATE = '{{ execution_date.strftime("%Y-%m") }}'

# table_schema = pa.schema(
#     [
#         ('VendorID',pa.int64()),
#         ('lpep_pickup_datetime',pa.timestamp('s')),
#         ('lpep_dropoff_datetime',pa.timestamp('s')),
#         ('store_and_fwd_flag',pa.string()),
#         ('RatecodeID',pa.int64()),
#         ('PULocationID',pa.int64()),
#         ('DOLocationID',pa.int64()),
#         ('passenger_count',pa.int64()),
#         ('trip_distance',pa.float64()),
#         ('fare_amount',pa.float64()),
#         ('extra',pa.float64()),
#         ('mta_tax',pa.float64()),
#         ('tip_amount',pa.float64()),
#         ('tolls_amount',pa.float64()),
#         ('ehail_fee',pa.float64()),
#         ('improvement_surcharge',pa.float64()),
#         ('total_amount',pa.float64()),
#         ('payment_type',pa.int64()),
#         ('trip_type',pa.int64()),
#         ('congestion_surcharge',pa.float64()),
#     ]
# )

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept CSV files")
        return
    dataset = pcsv.read_csv(src_file, convert_options=pcsv.ConvertOptions(column_types={'ehail_fee': 'float64'}))
    # dataset = dataset.cast(table_schema)
    ppq.write_table(dataset, src_file.replace('.csv', '.parquet'))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

dag = DAG(
    dag_id = 'green_taxi_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2021, 12, 1),
    catchup = True,
    max_active_runs=3
)

with dag:

    curl_data_task = BashOperator(
        task_id = 'curl_data_task',
        bash_command = f'curl -sSLf {URL_TEMPLATE} > {OUTPUT_CSV_FILE}'
    )

    format_to_parquet_task = PythonOperator(
        task_id = 'format_to_parquet_task',
        python_callable=format_to_parquet,
        op_kwargs= {'src_file' : OUTPUT_CSV_FILE}
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs_task",
        bucket=BUCKET,
        src=OUTPUT_PQ_FILE,
        dst=f'green/{EXEC_YEAR}/green_taxi_{EXEC_DATE}.parquet'
    )

    rm_files_task = BashOperator(
        task_id='rm_files_task',
        bash_command = f'rm {OUTPUT_CSV_FILE} {OUTPUT_PQ_FILE}'
    )

    curl_data_task >> format_to_parquet_task >> upload_to_gcs_task >> rm_files_task