import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utilities import database
from dotenv import load_dotenv
from datetime import timedelta, datetime

load_dotenv()

DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'main_database'
selenium_hub = '172.18.0.3'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedule_interval': '0 */12 * * *',
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2022, 11, 20),
        default_args=default_args,
        catchup=False
) as dag:
    task = PythonOperator(
        task_id='load_ad',
        python_callable=database.load_data_into_db,
        op_kwargs={
            'connector_id': CONN_ID,
            'selenium_hub_ip': selenium_hub
        }
    )

