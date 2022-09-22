import pendulum


from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator import PostgresOperator

headers = {
    'X-Nickname': 'a-v.shapowal',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
    'X-Cohort': '3'
}

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