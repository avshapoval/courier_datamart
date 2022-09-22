import pendulum
import logging
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

string_headers = Variable.get("CONNECTION_HEADERS")
headers = json.loads(string_headers)

pg_warehouse_conn_id = 'PG_WAREHOUSE_CONNECTION'
CUR_DATE = '{{ ts }}'


with DAG(
    dag_id="stg_to_dds",
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 8, 23, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    start = EmptyOperator(
        task_id = "start"
    )
    load_dm_couriers = PostgresOperator(
        task_id = "load_dm_couriers",
        postgres_conn_id=pg_warehouse_conn_id,
        sql='queries/load_dm_couriers.sql'
    )
    load_dm_orders = PostgresOperator(
        task_id = "load_dm_orders",
        postgres_conn_id=pg_warehouse_conn_id,
        sql ='queries/load_dm_orders.sql'
    )
    load_dm_restaurants = PostgresOperator(
        task_id = "load_dm_restaurants",
        postgres_conn_id=pg_warehouse_conn_id,
        sql = 'queries/load_dm_restaurants.sql'
    )
    load_fct_order_deliveries = PostgresOperator(
        task_id = "load_fct_order_deliveries",
        postgres_conn_id=pg_warehouse_conn_id,
        sql = 'queries/load_fct_order_deliveries.sql'
    )
    start >> [load_dm_couriers, load_dm_orders, load_dm_restaurants]  >> load_fct_order_deliveries