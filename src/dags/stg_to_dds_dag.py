import pendulum
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.postgres_operator import PostgresOperator

log = logging.getLogger(__name__)

headers = {
    'X-Nickname': 'a-v.shapowal',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f',
    'X-Cohort': '3'
}

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
        sql="""
            INSERT INTO dds.dm_couriers (id, object_id, name)
            SELECT id, object_id, (object_value::jsonb) ->> 'name' AS courier_name 
            FROM stg.couriersystem_couriers cc
            WHERE id > (SELECT max((workflow_settings::jsonb->>'max_id')::int) FROM dds.srv_wf_settings WHERE workflow_key = 'dm_couriers');

            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
            SELECT 'dm_couriers', 
                '{"max_id": "'|| (SELECT max(id) FROM dds.dm_couriers) ||'"}';
        """
    )
    load_dm_orders = PostgresOperator(
        task_id = "load_dm_orders",
        postgres_conn_id=pg_warehouse_conn_id,
        sql = """
            INSERT INTO dds.dm_orders(order_key, order_ts, "sum")
            SELECT object_id, 
                ((object_value::jsonb) ->>  'order_ts')::timestamp AS order_ts,
                ((object_value::jsonb) ->> 'sum')::numeric(14, 2) AS "sum"
            FROM stg.couriersystem_deliveries cd
            WHERE ((object_value::jsonb) ->>  'order_ts')::timestamp > 
                COALESCE(
                    (SELECT MAX((workflow_settings::jsonb->>'update_ts')::timestamp) 
                    FROM dds.srv_wf_settings sws 
                    WHERE sws.workflow_key = 'dm_orders'),
                '1980-01-01');

            WITH max_date AS (
                SELECT max(order_ts)::text "update_ts"
                    FROM dds.dm_orders
            )
            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
            SELECT 
                'dm_orders',
                '{"update_ts": "'|| (SELECT update_ts FROM max_date LIMIT 1) ||'"}';
        """
    )
    load_dm_restaurants = PostgresOperator(
        task_id = "load_dm_restaurants",
        postgres_conn_id=pg_warehouse_conn_id,
        sql = """
            INSERT INTO dds.dm_restaurants (id, object_id, "name")
            SELECT id, object_id, (object_value::jsonb) ->> 'name' AS r_name
            FROM stg.couriersystem_restaurants cr
            WHERE id > (SELECT max((workflow_settings::jsonb->>'max_id')::int) FROM dds.srv_wf_settings WHERE workflow_key = 'dm_restaurants');

            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
            SELECT 'dm_restaurants', 
                '{"max_id": "'|| (SELECT max(id) FROM dds.dm_restaurants) ||'"}'; 
        """
    )
    load_fct_order_deliveries = PostgresOperator(
        task_id = "load_fct_order_deliveries",
        postgres_conn_id=pg_warehouse_conn_id,
        sql = """
            INSERT INTO dds.fct_order_deliveries (delivery_key, order_id, courier_id, delivery_ts, tip_sum, rating, address, restaurant_id)
            SELECT cd.object_id,
                do2.id,
                dc.id,
                ((object_value::jsonb) ->> 'delivery_ts')::timestamp AS dts,
                ((object_value::jsonb) ->> 'tip_sum')::numeric(14, 2) tip,
                ((object_value::jsonb) ->> 'rate')::numeric(14, 2) rat,
                (object_value::jsonb) ->> 'address' addr,
                1 -- В заказах не указан ресторан, поэтому считаем что все заказы от первого ресторана.
            FROM stg.couriersystem_deliveries cd
                LEFT JOIN dds.dm_orders do2 ON ((cd.object_value::jsonb) ->> 'order_id') = do2.order_key 
                LEFT JOIN dds.dm_couriers dc ON ((cd.object_value::jsonb) ->> 'courier_id') = dc.object_id
            WHERE ((object_value::jsonb) ->>  'delivery_ts')::timestamp > 
                COALESCE(
                    (SELECT MAX((workflow_settings::jsonb->>'update_ts')::timestamp) 
                    FROM dds.srv_wf_settings sws 
                    WHERE sws.workflow_key = 'fct_order_deliveries'),
                '1980-01-01');
                
            WITH max_date AS (
                SELECT max(delivery_ts)::text "update_ts"
                    FROM dds.fct_order_deliveries fod 
            )
            INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
            SELECT 
                'fct_order_deliveries',
                '{"update_ts": "'|| (SELECT update_ts FROM max_date LIMIT 1) ||'"}';
        """
    )
    start >> [load_dm_couriers, load_dm_orders, load_dm_restaurants]  >> load_fct_order_deliveries