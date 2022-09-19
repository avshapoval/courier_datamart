import pendulum
import requests
from datetime import datetime
import json
import logging
from datetime import datetime
import dateutil.parser as dp

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

headers = {
    'X-Nickname': 'a-v.shapowal',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
    'X-Cohort': '3'
}

pg_warehouse_conn_id = 'PG_WAREHOUSE_CONNECTION'
CUR_DATE = '{{ ts }}'

def get_docs(collection_name, from_date=None, to_date=None):

    offset = 0
    all_docs = []

    while True:
        url = f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{collection_name}?offset={offset}"
        
        if from_date:
            url += f'&from={from_date}'

        if to_date:
            url += f'&to={to_date}'

        
        r = requests.get(url, headers=headers)

        if r.status_code != 200:
            log.warning(f"Bad request at offset {offset} in collection {collection_name}")
            break

        if r.text == '[]':
            log.log(1, f"No more records at offset {offset} in collection {collection_name}")
            break

        docs = json.loads(r.text)

        all_docs.extend(docs)
        offset += 50
    return all_docs

def load_from_api_full(collection_name, id_column: str): 
    page = 0
    all_docs = get_docs(collection_name=collection_name)
    records = [
        {
            "object_id": str(doc[id_column]),
            "object_value": json.dumps(doc),
            #"update_ts": datetime.datetime.strftime(doc["update_ts"], "%Y-%m-%dT%H:%M:%S.%f%z")
        }
        for doc in all_docs
    ]

    pg_hook = PostgresHook(pg_warehouse_conn_id)

    for record in records:
        pg_hook.run(
            f"""
            insert into stg.couriersystem_{collection_name} (object_id, object_value)
            values ('{record["object_id"]}', '{record["object_value"]}')
            on conflict (object_id)
            do update
            set object_id = EXCLUDED.object_id,
                object_value = EXCLUDED.object_value;
        """
    )
    
def load_from_api_inc(ti, collection_name, id_column :str, to_date: str = None):
    from_date = '1980-01-01T00:00:00.0'
    from_date = ti.xcom_pull(task_ids=f"get_last_delivery_date", key=f"update_ts.couriersystem.{collection_name}")
    from_date = datetime.strftime(datetime.strptime(from_date, '%Y-%m-%dT%H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S')
    all_docs = get_docs(collection_name=collection_name, from_date=from_date, to_date=to_date)
    records = [
        {
            "object_id": str(doc[id_column]),
            "object_value": json.dumps(doc),
            "update_ts": doc["order_ts"]
        }
        for doc in all_docs
    ]

    max_date = max([dp.parse(record["update_ts"]) for record in records])
        
    pg_hook = PostgresHook(pg_warehouse_conn_id)

    for record in records:
        pg_hook.run(
            f"""
            insert into stg.couriersystem_{collection_name} (object_id, object_value, update_ts)
            values ('{record["object_id"]}', '{record["object_value"]}', '{record["update_ts"]}')
            on conflict (object_id)
            do update
            set object_id = EXCLUDED.object_id,
                object_value = EXCLUDED.object_value,
                update_ts = EXCLUDED.update_ts;
        """
    )

    pg_hook.run(
        """
        insert into stg.srv_wf_settings (workflow_key, workflow_settings)
        values ('couriersystem.%s', '{"update_ts": "%s"}') """
        % (collection_name, max_date)
    )

def get_last_date(collection_name, ti):
    query = "select coalesce(max((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01 00:00:00') " +\
        "from stg.srv_wf_settings where workflow_key = 'couriersystem.%s'"%(collection_name)

    update_ts = PostgresHook(postgres_conn_id=pg_warehouse_conn_id).get_first(query)[0]
    
    ti.xcom_push(key=f"update_ts.couriersystem.{collection_name}",value=datetime.strftime(update_ts, "%Y-%m-%dT%H:%M:%S.%f%z"))


with DAG(
    dag_id="api_to_stg",
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 8, 23, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    start = EmptyOperator(
        task_id="start"
    )
    get_last_delivery_date = PythonOperator(
        task_id="get_last_delivery_date",
        python_callable=get_last_date,
        op_kwargs={
            "collection_name": "deliveries"
        }
    )
    load_api_restaurants = PythonOperator(
        task_id = "load_restaurants",
        python_callable=load_from_api_full,
        op_kwargs={
            "collection_name": "restaurants",
            "id_column": "_id"
        }
    )
    load_api_deliveries = PythonOperator(
        task_id = "load_deliveries",
        python_callable = load_from_api_inc,
        op_kwargs={
            "collection_name": "deliveries",
            "id_column": "order_id",
            "to_date": datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
            }
    )
    load_api_couriers = PythonOperator(
        task_id = "load_couriers",
        python_callable=load_from_api_full,
        op_kwargs={
            "collection_name": "couriers",
            "id_column": "_id"
        }
    )
    start >> [load_api_couriers, load_api_restaurants, get_last_delivery_date]
    get_last_delivery_date>>load_api_deliveries
