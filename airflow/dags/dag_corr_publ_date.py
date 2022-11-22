from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from utilities.new_columns import add_correct_publication_date, insert_publ_date_at_db

import os

DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'main_database'

with DAG(
        dag_id=DAG_ID,
        schedule_interval='0 11,23 * * *',
        start_date=datetime(2022, 11, 20),
        catchup=False
) as dag:
    # 1. get ads to correct ads
    task_get_correct_date = PythonOperator(
        task_id='get_ads_to_corr_date',
        python_callable=add_correct_publication_date,
        op_kwargs={
            'connector_id': CONN_ID
        }
    )

    # 2. Insert class to  db
    task_insert_correct_date = PythonOperator(
        task_id='save_class',
        python_callable=insert_publ_date_at_db,
        op_kwargs={
            'connector_id': CONN_ID
        }
    )

    task_get_correct_date >> task_insert_correct_date
