from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import simplejson as json
from utilities.predict import get_ads_to_predict, insert_class_at_db

import os

DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'main_database'





with DAG(
        dag_id=DAG_ID,
        start_date=datetime(2022, 11, 20),
        schedule_interval='0 11,23 * * *',
        catchup=False
) as dag:
    # 1. Get ads to predict
    task_get_ads_to_predict = PythonOperator(
        task_id='get_ad',
        python_callable=get_ads_to_predict,
        op_kwargs={
            'connector_id': CONN_ID,
        }
    )

    # 2. predict_ads
    task_predict_ads = SimpleHttpOperator(
        task_id='predict_class',
        http_conn_id='fast_api',
        endpoint='predict_class/',
        method="POST",
        data="{{ti.xcom_pull(task_ids='get_ad', key='ads_to_push')}}",
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # 3. Insert class to  db
    task_save_class = PythonOperator(
        task_id='save_class',
        python_callable=insert_class_at_db,
        op_kwargs={
            'connector_id': CONN_ID
        }
    )

    task_get_ads_to_predict >> task_predict_ads >> task_save_class
