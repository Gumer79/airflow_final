from datetime import timedelta, datetime
import psycopg2
from airflow.hooks.base import BaseHook
import dateparser
import simplejson as json
from datetime import datetime


def get_db_url(connector_id: str) -> str:
    connection = BaseHook.get_connection(connector_id)

    return f'user={connection.login} password={connection.password} host={connection.host} ' \
           f'port={connection.port} dbname={connection.schema}'


def add_correct_publication_date(connector_id: str, **kwargs):
    connection = psycopg2.connect(get_db_url(connector_id))
    columns = ['ad_id', 'publication_date', 'parsing_date']
    query = f"""SELECT {', '.join(columns)} FROM ads where corr_publ_date is null;"""
    with connection.cursor() as curs:
        curs.execute(query)
        connection.commit()
        ads_to_correct = curs.fetchall()
    corr_publ_date = []
    ad_id = []
    for ad in ads_to_correct:
        if ad[1].split()[0] == 'сегодня':
            corr_publ_date.append(ad[2].strftime("%Y-%m-%d"))
        elif ad[1].split()[0] == 'вчера':
            corr_publ_date.append((ad[2] - timedelta(days=1)).strftime("%Y-%m-%d"))
        else:
            publication_date = dateparser.parse(ad[1]).strftime("%Y-%m-%d")
            corr_publ_date.append(publication_date)
        ad_id.append(ad[0])
    result = [{'ad_id': ad_id, 'corr_publ_date': corr_publ_date} for ad_id, corr_publ_date in
              zip(ad_id, corr_publ_date)]
    result = json.dumps(result)
    print(result)
    ti = kwargs['ti']
    ti.xcom_push(key="ads_to_push", value=result)


def insert_publ_date_at_db(connector_id: str, ti):
    connection = psycopg2.connect(get_db_url(connector_id))
    ads = ti.xcom_pull(task_ids=['get_ads_to_corr_date'], key="ads_to_push")
    ads = json.loads(ads[0])
    for ad in ads:
        try:
            query = f"""UPDATE ads SET corr_publ_date='{ad.get('corr_publ_date')}' WHERE ad_id={ad.get('ad_id')};"""
            with connection.cursor() as curs:
                curs.execute(query)
                connection.commit()
            print(f"[INFO] - {ad.get('ad_id')} corr_publ_date {ad.get('corr_publ_date')} inserted successfully.")
        except Exception as e:
            print(e)
            print(f"[INFO] - {ad.get('ad_id')} couldn\'t insert.")
    print("[INFO] - All publication date is corrrected.")


def get_ads_to_brand(connector_id: str, **kwargs):
    connection = psycopg2.connect(get_db_url(connector_id))
    columns = ['ad_id', 'name']
    query = f"""SELECT {', '.join(columns)} FROM ads where brand is null;"""
    with connection.cursor() as curs:
        curs.execute(query)
        connection.commit()
        ads_to_correct = curs.fetchall()
    corr_brand_names = []
    ad_id = []
    for ad in ads_to_correct:
        brand_name = ad[1].split()[0]
        corr_brand_names.append(brand_name)
        ad_id.append(ad[0])
    result = [{'ad_id': ad_id, 'brand': brand} for ad_id, brand in zip(ad_id, corr_brand_names)]
    result = json.dumps(result)
    print(result)
    ti = kwargs['ti']
    ti.xcom_push(key="ads_to_push", value=result)


def add_brand_to_db(connector_id: str, ti):
    connection = psycopg2.connect(get_db_url(connector_id))
    ads = ti.xcom_pull(task_ids=['get_ads_to_add_brand'], key="ads_to_push")
    ads = json.loads(ads[0])
    for ad in ads:
        try:
            query = f"""UPDATE ads SET brand='{ad.get('brand')}' WHERE ad_id={ad.get('ad_id')};"""
            with connection.cursor() as curs:
                curs.execute(query)
                connection.commit()
            print(f"[INFO] - {ad.get('ad_id')} brand {ad.get('brand')} inserted successfully.")
        except Exception as e:
            print(e)
            print(f"[INFO] - {ad.get('ad_id')} brand couldn\'t insert.")
    print("[INFO] - All publication date is corrrected.")