import logging
from pymongo.mongo_client import MongoClient
from urllib.parse import quote_plus as quote
from typing import List
import datetime
import pendulum
from bson.json_util import dumps
import psycopg2
import pytz

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.postgres_hook import PostgresHook

log = logging.getLogger(__name__)

pg_warehouse_conn_id = 'PG_WAREHOUSE_CONNECTION'

cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
db_user = Variable.get("MONGO_DB_USER")
db_pw = Variable.get("MONGO_DB_PASSWORD")
hosts = Variable.get("MONGO_DB_HOST")
rs = Variable.get("MONGO_DB_REPLICA_SET")
mongo_db = Variable.get("MONGO_DB_DATABASE_NAME")

class MongoConnect:
    def __init__(self,
                 cert_path: str,  # Путь до файла с сертификатом
                 user: str,  # Имя пользователя БД
                 pw: str,  # Пароль пользователя БД
                 hosts: List[str],  # Список хостов для подключения
                 rs: str,  # replica set.
                 auth_db: str,  # БД для аутентификации
                 main_db: str  # БД с данными
                 ) -> None:
        self.user = user
        self.pw = pw
        self.hosts = hosts
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    # Формируем строку подключения к MongoDB
    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=self.hosts,
            rs=self.replica_set,
            auth_src=self.auth_db)

    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db] 

def get_update_ts (collection_name, ti):

    query = "select coalesce(max((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01T00:00:00.0Z') " +\
        "from stg.srv_wf_settings where workflow_key = 'mongodb.%s'"%(collection_name)

    update_ts = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION').get_first(query)[0]
    update_ts = update_ts.replace(tzinfo = pytz.UTC)

    ti.xcom_push(key=f"update_ts.mongodb.{collection_name}",value=datetime.datetime.strftime(update_ts, "%Y-%m-%dT%H:%M:%S.%f%z"))
    
    

def load_collection(collection_name, ti):

    update_ts = ti.xcom_pull(task_ids=f"get_update_ts_{collection_name}", key=f"update_ts.mongodb.{collection_name}")    
    update_ts = datetime.datetime.strptime(update_ts, "%Y-%m-%dT%H:%M:%S.%f%z")

    # Объявляем параметры фильтрации
    filter = {'update_ts': {'$gt': update_ts}}

    # Объявляем параметры сортировки
    sort = [('update_ts', 1)]

    mongo_connect = MongoConnect(cert_path, db_user, db_pw, hosts, rs, mongo_db, mongo_db)
    dbs = mongo_connect.client()
    
    # Получаем коллекцию
    docs = list(dbs.get_collection(collection_name).find(filter=filter, sort=sort))

    if not docs:
        log.warn("NO DOCS FOUND")
        return

    max_date = docs[-1]["update_ts"]
    records = [
        {
            "object_id": str(doc["_id"]),
            "object_value": dumps(doc),
            "update_ts": datetime.datetime.strftime(doc["update_ts"], "%Y-%m-%dT%H:%M:%S.%f%z")
        }
        for doc in docs
    ]

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="de",
        user="jovyan",
        password="jovyan"
    )

    for record in records:
        cur = conn.cursor()
        cur.execute(
            f"""
            insert into stg.ordersystem_{collection_name} (object_id, object_value, update_ts)
            values ('{record["object_id"]}', '{record["object_value"]}', '{record["update_ts"]}');
        """
    )

    conn.commit()

    conn.close()

    conn_srv = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="de",
        user="jovyan",
        password="jovyan"
    )

    cur_srv = conn_srv.cursor()
    cur_srv.execute(
        """
        insert into stg.srv_wf_settings (workflow_key, workflow_settings)
        values ('mongodb.%s', '{"update_ts": "%s"}')
        """ % (collection_name, max_date)
    )

    conn_srv.commit()
    conn_srv.close()


with DAG(
    dag_id = "mongoDB_to_stg",
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 8, 23, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    start = EmptyOperator(task_id="start")

    load_users = PythonOperator(
        task_id = "load_mongo_users",
        python_callable=load_collection,
        op_kwargs={
            "collection_name": "users"
        }
    )

    load_orders = PythonOperator(
        task_id = "load_mongo_orders",
        python_callable=load_collection,
        op_kwargs={
            "collection_name": "orders"
        }
    )

    load_restaurants = PythonOperator(
        task_id = "load_mongo_restaurants",
        python_callable=load_collection,
        op_kwargs={
            "collection_name": "restaurants"
        }
    )

    get_update_ts_users = PythonOperator(
        task_id = "get_update_ts_users",
        python_callable=get_update_ts,
        op_kwargs={"collection_name": "users"}
    )

    get_update_ts_orders = PythonOperator(
        task_id = "get_update_ts_orders",
        python_callable=get_update_ts,
        op_kwargs={"collection_name": "orders"}
    )

    get_update_ts_restaurants = PythonOperator(
        task_id = "get_update_ts_restaurants",
        python_callable=get_update_ts,
        op_kwargs={"collection_name": "restaurants"}
    )

    start >> [
        get_update_ts_users, 
        get_update_ts_orders,
        get_update_ts_restaurants
    ]
    get_update_ts_users >> load_users
    get_update_ts_orders >> load_orders
    get_update_ts_restaurants >> load_restaurants