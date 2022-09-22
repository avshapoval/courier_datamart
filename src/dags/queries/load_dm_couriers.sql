INSERT INTO dds.dm_couriers (id, object_id, name)
SELECT id, object_id, (object_value::jsonb) ->> 'name' AS courier_name 
FROM stg.couriersystem_couriers cc
WHERE id > COALESCE((SELECT max((workflow_settings::jsonb->>'max_id')::int) FROM dds.srv_wf_settings WHERE workflow_key = 'dm_couriers'), 0);

INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
SELECT 'dm_couriers', 
    '{"max_id": "'|| (SELECT max(id) FROM dds.dm_couriers) ||'"}';