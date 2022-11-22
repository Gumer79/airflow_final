import requests
import psycopg2
from airflow.hooks.base import BaseHook
from airflow.providers.http.operators.http import SimpleHttpOperator
import simplejson as json


def get_db_url(connector_id: str) -> str:
    connection = BaseHook.get_connection(connector_id)

    return f'user={connection.login} password={connection.password} host={connection.host} ' \
           f'port={connection.port} dbname={connection.schema}'


def catch(func, handle=lambda e: e, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except:
        return None


def get_ads_to_predict(connector_id: str, **kwargs):
    connection = psycopg2.connect(get_db_url(connector_id))
    columns = ['ad_id', 'price', 'year', 'mileage', 'engine_power', 'engine_vol', 'n_owners']
    query = f"""SELECT {', '.join(map(str, columns))} FROM ads where kmeans is null;"""
    with connection.cursor() as curs:
        curs.execute(query)
        connection.commit()
        ads = curs.fetchall()
    result = []
    for ad in ads:
        if None in ad:
            pass
        else:
            ad = ['4' if x == '4+' else x for x in ad]
            ad = [catch(lambda: float(x) if "." in str(x) else int(x)) for x in ad]
            ad = dict(zip(columns, ad))
            result.append(ad)
    result = json.dumps(result)
    print(result)
    ti = kwargs['ti']
    ti.xcom_push(key="ads_to_push", value=result)


def insert_class_at_db(connector_id: str, ti):
    connection = psycopg2.connect(get_db_url(connector_id))
    predicted_ads = ti.xcom_pull(task_ids=['predict_class'])
    for ad in predicted_ads[0]:
        try:
            if ad.get('predicted_class') is None:
                query = f"""UPDATE ads SET kmeans = NULL WHERE ad_id={ad.get('ad_id')};"""
                with connection.cursor() as curs:
                    curs.execute(query)
                    connection.commit()
            else:
                query = f"""UPDATE ads SET kmeans = {ad.get('predicted_class')} WHERE ad_id={ad.get('ad_id')};"""
                with connection.cursor() as curs:
                    curs.execute(query)
                    connection.commit()
                print(f"[INFO] - {ad.get('ad_id')} class {ad.get('predicted_class')} inserted successfully.")
        except Exception as e:
            print(e)
            print(f"[INFO] - {ad.get('ad_id')} couldn\'t insert.")
