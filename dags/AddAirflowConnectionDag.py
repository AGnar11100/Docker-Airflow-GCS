from config.secret import path_to_json, project_name
from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Connection
import json

def add_connection(**kwargs):
    
    # thee connection 
    new_conn = Connection(
        conn_id='google_cloud_default',
        conn_type='google_cloud_platform'
    )

    # metadata on the connection: scope, project, key 
    conn_details = {
        # api interface
        'extra__google_cloud_platform__scope': 'https://www.googleapis.com/auth/cloud-platform',
        'extra__google_cloud_platform__project': project_name,
        'extra__google_cloud_platform__key_path': path_to_json
    }


    session = settings.Session()
    # check if the connection exists
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        # query a connection, return the connection w the same ID as the last
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        
        # update connection details just for consistency
        my_connection.set_extra(json.dumps(conn_details))
        session.commit()
    
    # if the connection doesnt exist
    else:
        # set it to the connection above for creation 
        new_conn.set_extra(json.dumps(conn_details))
        session.add(new_conn)
        session.commit()




# DAG details
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 3, 7), 
    'email' : ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries' : 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('AddGCPConnection', default_args=default_args, schedule_interval='@once')

with dag: 
    activateGCP = PythonOperator(
        task_id ='airflow_to_gcp_conn', 
        python_callable=add_connection,
        provide_context=True
    )

activateGCP