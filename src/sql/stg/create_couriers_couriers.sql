CREATE TABLE IF NOT EXISTS stg.couriersystem_couriers (
	id serial4 NOT NULL,
	object_id varchar NULL,
	object_value text NULL,
	CONSTRAINT couriersystem_couriers_object_id_key UNIQUE (object_id),
	CONSTRAINT couriersystem_couriers_pkey PRIMARY KEY (id)
);