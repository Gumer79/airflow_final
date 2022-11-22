from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utilities.new_columns import get_ads_to_brand, add_brand_to_db

import os

DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'main_database'

with DAG(
        dag_id=DAG_ID,
        schedule_interval='0 11,23 * * *',
        start_date=datetime(2022, 11, 20),
        catchup=False
) as dag:
    # 1. get ads to add brand
    task_get_ads_to_brand = PythonOperator(
        task_id='get_ads_to_add_brand',
        python_callable=get_ads_to_brand,
        op_kwargs={
            'connector_id': CONN_ID
        }
    )

    # 2. Insert brand to  db
    task_insert_brand = PythonOperator(
        task_id='save_class',
        python_callable=add_brand_to_db,
        op_kwargs={
            'connector_id': CONN_ID
        }
    )

    task_get_ads_to_brand >> task_insert_brand
