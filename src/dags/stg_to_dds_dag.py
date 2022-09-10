import pendulum
import logging

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models.variable import Variable


log = logging.getLogger(__name__)

pg_warehouse_conn_id = 'PG_WAREHOUSE_CONNECTION'


with DAG(
    dag_id = 'stg_to_dds',
    schedule_interval = '0/15 * * * *',
    start_date = pendulum.datetime(2022, 8, 26, tz="utc"),
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    fill_dm_users = PostgresOperator(
        task_id = "fill_dm_users",
        sql="""
        INSERT INTO dds.dm_users (user_id, user_name, user_login)
        SELECT 
	        bu.order_user_id,
	        ou.object_value::json->>'name' user_name,
	        ou.object_value::json->>'login' user_login
        FROM stg.ordersystem_users ou 
        INNER JOIN stg.bonussystem_users bu ON ou.object_id = bu.order_user_id
        WHERE bu.id > COALESCE( (SELECT MAX((workflow_settings::json->>'id')::int) FROM dds.srv_wf_settings WHERE workflow_key = 'dm_users'), 0);

        
        DO
        $do$
        BEGIN
            IF (SELECT COALESCE((SELECT MAX(bu.id) FROM stg.ordersystem_users ou 
        INNER JOIN stg.bonussystem_users bu ON ou.object_id = bu.order_user_id), 0)) != (SELECT COALESCE(MAX((workflow_settings::json->>'id')::int), 0) FROM dds.srv_wf_settings sws WHERE workflow_key = 'dm_users')
		         THEN
      		        INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
			        VALUES('dm_users', '{"id": ' || COALESCE((SELECT MAX(bu.id) FROM stg.ordersystem_users ou 
                            INNER JOIN stg.bonussystem_users bu ON ou.object_id = bu.order_user_id), 0)::TEXT || '}');
            END IF;
        END
        $do$
        """,
        postgres_conn_id=pg_warehouse_conn_id
    )

    fill_dm_restaurants = PostgresOperator(
        task_id="fill_dm_restaurants",
        sql = """
        WITH cte AS (
	        SELECT object_id, update_ts
	        FROM stg.ordersystem_restaurants
	        WHERE update_ts > (
		        SELECT COALESCE(MAX((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01')
		        FROM dds.srv_wf_settings sws 
		        WHERE workflow_key = 'dm_restaurants'
	        )
        )
        UPDATE dds.dm_restaurants 
        SET active_to = cte.update_ts
        FROM cte
        WHERE active_to = '2099-12-31' AND restaurant_id = cte.object_id;

        INSERT INTO dds.dm_restaurants
        SELECT
        	id,
        	object_id, 
        	object_value::json->>'name',
        	update_ts active_from,
        	'2099-12-31T00:00:00.0Z' active_to
        FROM stg.ordersystem_restaurants
        WHERE update_ts > (
        	SELECT COALESCE(MAX((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01')
        	FROM dds.srv_wf_settings sws 
        	WHERE workflow_key = 'dm_restaurants'
        );

        DO
        $do$
        BEGIN
            IF (SELECT MAX(active_from) FROM dds.dm_restaurants) != (SELECT COALESCE(MAX((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01')
        		    FROM dds.srv_wf_settings sws WHERE workflow_key = 'dm_restaurants') THEN
      		    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
			    VALUES('dm_restaurants', '{"update_ts": "' || (SELECT MAX(active_from) FROM dds.dm_restaurants)::TEXT || '"}');
            END IF;
        END
        $do$
        """,
        postgres_conn_id=pg_warehouse_conn_id
    )

    fill_dm_timestamps = PostgresOperator(
        task_id="fill_dm_timestamps",
        sql="""
        WITH cte AS (
	        SELECT DISTINCT (object_value::json->'update_ts'->>'$date')::timestamp update_ts
	        FROM stg.ordersystem_orders oo 
	        WHERE (object_value::json->>'final_status')::TEXT IN ('CLOSED', 'CANCELLED')
        )
        INSERT INTO dds.dm_timestamps (ts, "year", "month", "day", "time", "date")
        SELECT 
	        update_ts,
	        EXTRACT (YEAR FROM update_ts),
	        EXTRACT (MONTH FROM update_ts),
	        EXTRACT (DAY FROM update_ts),
	        CAST (update_ts AS time),
	        CAST (update_ts AS date)
        FROM cte
        WHERE update_ts > (
		    SELECT COALESCE(max((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01')
		    FROM dds.srv_wf_settings 
		    WHERE workflow_key = 'dm_timestamps'
	    );
	
        DO
        $do$
        BEGIN
            IF (SELECT MAX(ts) FROM dds.dm_timestamps) != 
            	(SELECT COALESCE(MAX((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01')
        		    FROM dds.srv_wf_settings sws WHERE workflow_key = 'dm_timestamps') THEN
        		    
      		    INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
			    VALUES('dm_timestamps', '{"update_ts": "' || (SELECT MAX(ts) FROM dds.dm_timestamps)::TEXT || '"}');
            END IF;
        END
        $do$;
        """,
        postgres_conn_id=pg_warehouse_conn_id
    )
    
    fill_dm_products = PostgresOperator(
        task_id="fill_dm_products",
        sql="""--INSERT INTO dds.dm_products
WITH updated_rests AS(
	SELECT id, object_id, json_array_elements_text(object_value::json->'menu')::json menu, update_ts 
	FROM stg.ordersystem_restaurants or3 
	WHERE update_ts > (
		SELECT COALESCE(max((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01T00:00:00.0Z') 
		FROM dds.srv_wf_settings sws 
		WHERE workflow_key = 'dm_products'
	)
)
UPDATE dds.dm_products 
SET active_to = updated_rests.update_ts
FROM updated_rests
WHERE active_to = '2099-12-31' AND restaurant_id = updated_rests.id AND product_id = menu->'_id'->>'$oid';

WITH updated_rests AS(
	SELECT id, object_id, json_array_elements_text(object_value::json->'menu')::json menu, update_ts 
	FROM stg.ordersystem_restaurants or3 
	WHERE update_ts > (
		SELECT COALESCE(max((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01T00:00:00.0Z') 
		FROM dds.srv_wf_settings sws 
		WHERE workflow_key = 'dm_products'
	)
)
INSERT INTO dds.dm_products 
SELECT
	(menu->>'id')::int,
	dr.id,
	menu->'_id'->>'$oid',
	menu->>'name',
	(menu->>'price')::numeric,
	dr.active_from,
	'2099-12-31'
FROM updated_rests ur
INNER JOIN dds.dm_restaurants dr ON ur.id = dr.id;


DO
$do$
	BEGIN
		IF (SELECT COALESCE(max((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01T00:00:00.0Z')
			FROM dds.srv_wf_settings sws 
			WHERE workflow_key = 'dm_products') != (SELECT max(active_from) FROM dds.dm_products dp) THEN
				INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
			    VALUES('dm_products', '{"update_ts": "' || (SELECT MAX(active_from) FROM dds.dm_products)::TEXT || '"}');
			
		END IF; 
	END
$do$;
        """,
        postgres_conn_id=pg_warehouse_conn_id
    )

    fill_dm_orders = PostgresOperator(
        task_id="fill_dm_orders",
        sql="""WITH orders_json as(
	SELECT object_value::json object_value, update_ts 
	FROM stg.ordersystem_orders oo 
)
INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
SELECT
	du.id uid,
	dr.id rid,
	dt.id,
	oj.object_value -> '_id' ->> '$oid' okey,
	oj.object_value ->> 'final_status' ost
FROM orders_json oj
	INNER JOIN dds.dm_users du ON oj.object_value -> 'user' -> 'id' ->> '$oid' = du.user_id
	INNER JOIN dds.dm_restaurants dr ON oj.object_value -> 'restaurant' -> 'id' ->> '$oid' = dr.restaurant_id 
	INNER JOIN dds.dm_timestamps dt ON update_ts = dt.ts 
WHERE update_ts > (
	SELECT COALESCE (max((workflow_settings::json ->> 'update_ts')::timestamp), '1980-01-01')
	FROM dds.srv_wf_settings
	WHERE workflow_key = 'dm_orders'
);

DO
$do$
	BEGIN
		IF (SELECT COALESCE(max((workflow_settings::json->>'update_ts')::timestamp), '1980-01-01T00:00:00.0Z')
			FROM dds.srv_wf_settings sws 
			WHERE workflow_key = 'dm_orders') != 
			(SELECT max(dp.ts) 
			FROM dds.dm_timestamps dp INNER JOIN dds.dm_orders do2 ON dp.id = do2.timestamp_id) THEN
				INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
			    VALUES('dm_orders', '{"update_ts": "' || (SELECT max(dp.ts) 
					FROM dds.dm_timestamps dp 
					INNER JOIN dds.dm_orders do2 ON dp.id = do2.timestamp_id)::TEXT || '"}');
		END IF; 
	END
$do$;
        """,
        postgres_conn_id=pg_warehouse_conn_id
    )

    fill_fct_product_sales = PostgresOperator(
        task_id="fill_fct_product_sales",
        postgres_conn_id=pg_warehouse_conn_id,
        sql="""
        WITH event_value_json as(
	SELECT event_ts , event_value::jsonb, jsonb_array_elements(event_value::jsonb->'product_payments') product_arr
	FROM stg.bonussystem_events be 
	WHERE event_type = 'bonus_transaction'
)
INSERT INTO dds.fct_product_sales (product_id, order_id, "count", "price", total_sum, bonus_payment, bonus_grant)
SELECT 
	dp.id,
	do2.id,
	sum((product_arr ->> 'quantity')::int),
	(product_arr ->> 'price')::numeric,
	sum((product_arr ->> 'product_cost')::numeric),
	sum((product_arr ->> 'bonus_payment')::numeric),
	sum((product_arr ->> 'bonus_grant')::NUMERIC)
FROM event_value_json evj
	INNER JOIN dds.dm_products dp ON dp.product_id = (evj.product_arr ->> 'product_id')::text
	INNER JOIN dds.dm_orders do2 ON do2.order_key = (evj.event_value->>'order_id')::TEXT
WHERE event_ts  > (SELECT COALESCE (MAX((workflow_settings::jsonb->>'update_ts')::timestamp), '1980-01-01') FROM dds.srv_wf_settings sws WHERE workflow_key='fct_product_sales')
GROUP BY dp.id, do2.id, (product_arr ->> 'price')::NUMERIC;

DO
$do$
	BEGIN
		IF (SELECT COALESCE (MAX((workflow_settings::jsonb->>'update_ts')::timestamp), '1980-01-01') FROM dds.srv_wf_settings sws WHERE workflow_key='fct_product_sales') != 
			(SELECT max(event_ts) FROM stg.bonussystem_events be) THEN
				INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
			    VALUES('fct_product_sales', '{"update_ts": "' || (SELECT max(event_ts) FROM stg.bonussystem_events be)::TEXT || '"}');
		END IF; 
	END
$do$; 
        """
    )

    fill_dm_users >> fill_dm_restaurants >> fill_dm_timestamps >> fill_dm_products >> fill_dm_orders >> fill_fct_product_sales
