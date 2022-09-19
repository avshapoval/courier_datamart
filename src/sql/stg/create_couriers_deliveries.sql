CREATE TABLE IF NOT EXISTS stg.couriersystem_deliveries (
	id serial4 NOT NULL,
	object_id varchar NULL,
	object_value text NULL,
	update_ts timestamp NULL,
	CONSTRAINT couriersystem_deliveries_object_id_key UNIQUE (object_id),
	CONSTRAINT couriersystem_deliveries_pkey PRIMARY KEY (id)
);