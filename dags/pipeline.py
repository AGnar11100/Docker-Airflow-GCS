from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from config.secret import project_name, gcs_service_acc, gcs_bucket_raw, gcs_bucket_transform, gcs_cluster_name, my_region, my_dataset_name, local_script_loc, local_csv_loc
from google.cloud import bigquery
import os
import requests

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

SERVICE_ACCOUNT = gcs_service_acc
BUCKET_RAW = gcs_bucket_raw
BUCKET_TRANSFORMED = gcs_bucket_transform
CLUSTER_NAME = gcs_cluster_name
REGION = my_region
PROJECT_ID = project_name
DATASET_NAME = my_dataset_name
TABLE_NAME = 'vehicles'
SCRIPT_LOCATION = local_script_loc
LOCAL_CSV = local_csv_loc

FILES_TO_DOWNLOAD = [
    "https://data.ca.gov/dataset/15179472-adeb-4df6-920a-20640d02b08c/resource/d599c3d3-87af-4e8c-8694-9c01f49e3d93/download/vehicle-fuel-type-count-by-zip-code-20231.csv",
    "https://data.ca.gov/dataset/15179472-adeb-4df6-920a-20640d02b08c/resource/9aa5b4c5-252c-4d68-b1be-ffe19a2f1d26/download/vehicle-fuel-type-count-by-zip-code-2022.csv",
    "https://data.ca.gov/dataset/15179472-adeb-4df6-920a-20640d02b08c/resource/1856386b-a196-4e7c-be81-44174e29ad50/download/vehicle-fuel-type-count-by-zip-code-2022.csv",
    "https://data.ca.gov/dataset/15179472-adeb-4df6-920a-20640d02b08c/resource/888bbb6c-09b4-469c-82e6-1b2a47439736/download/vehicle-fuel-type-count-by-zip-code-2021.csv",
    "https://data.ca.gov/dataset/15179472-adeb-4df6-920a-20640d02b08c/resource/4254a06d-9937-4083-9441-65597dd267e8/download/vehicle-count-as-of-1-1-2020.csv",
    "https://data.ca.gov/dataset/15179472-adeb-4df6-920a-20640d02b08c/resource/d304108a-06c1-462f-a144-981dd0109900/download/vehicle-fuel-type-count-by-zip-code.csv"
]

def download_file(url, local_path):
    response = requests.get(url, allow_redirects=True)
    with open(local_path, 'wb') as file:
        file.write(response.content)

def create_dataset():
    hook = BigQueryHook()
    client = hook.get_client()
    dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION
    client.create_dataset(dataset, exists_ok=True)

def create_table():
    hook = BigQueryHook()
    client = hook.get_client()
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
    schema = [
        bigquery.SchemaField("ZIP_Code", "INTEGER"),
        bigquery.SchemaField("Make", "STRING"),
        bigquery.SchemaField("Model_Year", "STRING"),
        bigquery.SchemaField("Fuel", "STRING"),
        bigquery.SchemaField("Duty", "STRING"),
        bigquery.SchemaField("Vehicles", "INTEGER"),
        bigquery.SchemaField("County", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

with DAG(
    dag_id='ca_registered_vehicles_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['current project'],
) as dag:

    create_raw_bucket = GCSCreateBucketOperator(
        task_id='create_raw_bucket',
        bucket_name=BUCKET_RAW,
    )

    create_transformed_bucket = GCSCreateBucketOperator(
        task_id='create_transformed_bucket',
        bucket_name=BUCKET_TRANSFORMED,
    )

    download_tasks = []
    for i, url in enumerate(FILES_TO_DOWNLOAD):
        local_file_path = f'/tmp/{os.path.basename(url)}'
        download_task = PythonOperator(
            task_id=f'download_file_{i}',
            python_callable=download_file,
            op_args=[url, local_file_path],
        )
        download_tasks.append(download_task)

    upload_files_to_raw = [
        LocalFilesystemToGCSOperator(
            task_id=f'upload_file_{i}_to_raw',
            src=f'/tmp/{os.path.basename(url)}',
            dst=f'{i}_{os.path.basename(url)}',  # Append index to the filename
            bucket=BUCKET_RAW,
        ) for i, url in enumerate(FILES_TO_DOWNLOAD)
    ]

    upload_local_csv_to_raw = LocalFilesystemToGCSOperator(
        task_id='upload_local_csv_to_raw',
        src=LOCAL_CSV,
        dst='ZIP-COUNTY-FIPS_2021.csv',
        bucket=BUCKET_RAW,
    )

    upload_script_to_transformed = LocalFilesystemToGCSOperator(
        task_id='upload_script_to_transformed',
        src=SCRIPT_LOCATION,
        dst='transform.py',
        bucket=BUCKET_TRANSFORMED,
    )

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config={
            'gce_cluster_config': {
                'service_account': SERVICE_ACCOUNT,
            },
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-4',
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-4',
            },
        },
    )

    pyspark_job = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': f'gs://{BUCKET_TRANSFORMED}/transform.py',
            'args': [
                f'gs://{BUCKET_RAW}/',
                f'gs://{BUCKET_TRANSFORMED}/transformed_data/'
            ],
        },
    }

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_bigquery_dataset = PythonOperator(
        task_id='create_bigquery_dataset',
        python_callable=create_dataset,
    )

    create_bigquery_table = PythonOperator(
        task_id='create_bigquery_table',
        python_callable=create_table,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=BUCKET_TRANSFORMED,
        source_objects=['transformed_data/*'],
        destination_project_dataset_table=f'{PROJECT_ID}:{DATASET_NAME}.{TABLE_NAME}',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    create_raw_bucket >> download_tasks
    for i in range(len(download_tasks)):
        download_tasks[i] >> upload_files_to_raw[i]

    create_raw_bucket >> upload_local_csv_to_raw
    create_transformed_bucket >> upload_script_to_transformed

    for i in range(len(upload_files_to_raw)):
        upload_files_to_raw[i] >> create_dataproc_cluster

    upload_local_csv_to_raw >> create_dataproc_cluster
    upload_script_to_transformed >> create_dataproc_cluster

    create_dataproc_cluster >> submit_pyspark_job
    submit_pyspark_job >> delete_dataproc_cluster
    delete_dataproc_cluster >> create_bigquery_dataset
    create_bigquery_dataset >> create_bigquery_table
    create_bigquery_table >> load_to_bigquery

