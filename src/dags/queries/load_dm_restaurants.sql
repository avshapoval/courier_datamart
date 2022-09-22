INSERT INTO dds.dm_restaurants (id, object_id, "name")
SELECT id, object_id, (object_value::jsonb) ->> 'name' AS r_name
FROM stg.couriersystem_restaurants cr
WHERE id > COALESCE((SELECT max((workflow_settings::jsonb->>'max_id')::int) FROM dds.srv_wf_settings WHERE workflow_key = 'dm_restaurants'), 0);

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
SELECT 'dm_restaurants', 
    '{"max_id": "'|| (SELECT max(id) FROM dds.dm_restaurants) ||'"}'; 