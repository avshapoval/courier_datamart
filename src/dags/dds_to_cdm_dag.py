import pendulum
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable

string_headers = Variable.get("CONNECTION_HEADERS")
headers = json.loads(string_headers)

pg_warehouse_conn_id = 'PG_WAREHOUSE_CONNECTION'
CUR_DATE = '{{ ts }}'

with DAG(
    dag_id = "dds_to_cdm",
    schedule_interval="0/15 * * * *",
    start_date = pendulum.datetime(2022, 9, 19),
    is_paused_upon_creation=False,
    catchup=False
) as dag:
    start = EmptyOperator(
        task_id = "start"
    )
    load_dm_courier_ledger = PostgresOperator(
        task_id = "load_dm_courier_ledger",
        postgres_conn_id=pg_warehouse_conn_id,
        sql = 'queries/load_dm_courier_ledger.sql'
    )

    start >> load_dm_courier_ledger