from distutils.log import Log
import logging

import json
import pandas as pd
import pendulum

from typing import List
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable


log = logging.getLogger(__name__)

pg_source_conn_id = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
pg_warehouse_conn_id = 'PG_WAREHOUSE_CONNECTION'


def runner(source_hook, target_hook, sql, target_table):
    target_hook.run(f"TRUNCATE TABLE {target_table}") 
    df = source_hook.get_pandas_df(sql)
    df.set_index('id')
    target_hook.insert_rows(target_table, df.values, target_fields=df.columns.tolist(),commit_every=100)#, replace=True, replace_index='id')

def event_loader(source_hook:PostgresHook, target_hook:PostgresHook):
    max_loaded_id = target_hook.get_first(sql=f"SELECT MAX(id) FROM stg.bonussystem_events;")[0]
    max_loaded_id = 0 if max_loaded_id is None else int(max_loaded_id)
    df_to_load = source_hook.get_pandas_df(f"SELECT * FROM public.outbox WHERE id > {max_loaded_id};")
    
    df_to_load.to_sql('bonussystem_events', index=False, con=target_hook.get_sqlalchemy_engine(), if_exists='append', schema='stg')
    df_srv = df_to_load[df_to_load['id'] == df_to_load['id'].max()]

    df_srv['workflow_key'] = 'public.outbox'
    df_srv['workflow_settings'] = json.dumps({"event_ts": df_srv[['event_ts', 'event_type', 'event_value']].to_json(orient='records')})

    df_srv[['id', 'workflow_key', 'workflow_settings']].to_sql('srv_wf_settings', index=False, con=target_hook.get_sqlalchemy_engine(), if_exists='append', schema='stg')


with DAG(
    dag_id="source_to_stg",
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 8, 23, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False
) as dag:

    start = EmptyOperator(task_id="start")

    load_ranks = PythonOperator(
        task_id="load_ranks",
        python_callable=runner,
        op_kwargs={
            "source_hook": PostgresHook(pg_source_conn_id),
            "target_hook": PostgresHook(pg_warehouse_conn_id),
            "sql": "SELECT id, name, bonus_percent, min_payment_threshold from public.ranks",
            "target_table": "stg.bonussystem_ranks"}
    )
    
    load_users = PythonOperator(
        task_id="load_users",
        python_callable=runner,
        op_kwargs={
            "source_hook": PostgresHook(pg_source_conn_id),
            "target_hook": PostgresHook(pg_warehouse_conn_id),
            "sql": "SELECT id, order_user_id from public.users",
            "target_table": "stg.bonussystem_users"}
    )
    
    events_load = PythonOperator(
        task_id = "load_events",
        python_callable=event_loader,
        op_kwargs={
            "source_hook": PostgresHook(pg_source_conn_id),
            "target_hook": PostgresHook(pg_warehouse_conn_id),
            "sql": "SELECT id, order_user_id from public.users",
            "target_table": "stg.srv_wf_settings"
        }
    )

    start >> [load_ranks, load_users] >> events_load 
 

