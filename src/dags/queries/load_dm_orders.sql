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