import pendulum
import requests
from datetime import datetime
import json
import csv
import logging

from airflow import DAG

log = logging.getLogger(__name__)

headers = {
    'X-Nickname': 'a-v.shapowal',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
    'X-Cohort': '3'
}

def load_from_api(collection_name, id: str, from_date: str = None): # Доделать загрузку в той же функции
    page = 0
    all_docs = []
    while True:
        url = f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{collection_name}?offset={page}"

        if from_date:
            url += f'&from={from_date}'

        r = requests.get(url, headers=headers)

        if r.status_code != 200:
            log.warning(f"Bad request at page {page} in collection {collection_name}")
            break

        if not r.text:
            log.log(f"No more records at page {page} in collection {collection_name}")
            break

        docs = json.loads(r.text)

        for doc in docs: # Итерация по списку словарей страницы {{ page }}
            all_docs.append(doc)
        page += 1
    
    records = [
        {
            "object_id": str(doc[id]),
            "object_value": json.dumps(doc),
            #"update_ts": datetime.datetime.strftime(doc["update_ts"], "%Y-%m-%dT%H:%M:%S.%f%z")
        }
        for doc in all_docs
    ]
    print(records)

load_from_api('restaurants', id="_id")
load_from_api('deliveries', id="order_id")
load_from_api('couriers', id="_id")


with DAG(
    dag_id="api_to_stg",
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 8, 23, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    pass