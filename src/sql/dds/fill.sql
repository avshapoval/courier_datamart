--dm_couriers
INSERT INTO dds.dm_couriers (object_id, name)
SELECT object_id, (object_value::jsonb) ->> 'name' AS courier_name 
FROM stg.couriersystem_couriers cc;

--dm_orders
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
	'{"update_ts": "'|| (SELECT update_ts FROM max_date LIMIT 1) ||'"}' 

--dm_restaurants
INSERT INTO dds.dm_restaurants (object_id, "name")
SELECT object_id, (object_value::jsonb) ->> 'name' AS r_name
FROM stg.couriersystem_restaurants cr 


--fct_order_deliveries
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
