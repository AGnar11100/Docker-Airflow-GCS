from config.secret import path_to_json, project_name
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Connection
import json

def add_connection(**kwargs):
    # Create a new connection object
    new_conn = Connection(
        conn_id='google_cloud_default',
        conn_type='google_cloud_platform'
    )

    # Define connection details
    conn_details = {
        'extra__google_cloud_platform__scope': 'https://www.googleapis.com/auth/cloud-platform',
        'extra__google_cloud_platform__project': project_name,
        'extra__google_cloud_platform__key_path': path_to_json
    }

    session = settings.Session()

    # Check if the connection already exists
    existing_conn = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first()
    if existing_conn:
        # Update connection details if it exists
        existing_conn.set_extra(json.dumps(conn_details))
    else:
        # Add new connection if it does not exist
        new_conn.set_extra(json.dumps(conn_details))
        session.add(new_conn)
    
    session.commit()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 7),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Initialize the DAG
dag = DAG('AddGCPConnection', default_args=default_args, schedule_interval='@once')

# Define tasks within the DAG
with dag:
    connect_to_gcs = PythonOperator(
        task_id='airflow_to_gcp_conn',
        python_callable=add_connection,
        provide_context=True
    )

connect_to_gcs