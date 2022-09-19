CREATE TABLE IF NOT EXISTS stg.couriersystem_restaurants (
	id serial4 NOT NULL,
	object_id varchar NULL,
	object_value text NULL,
	CONSTRAINT couriersystem_restaurants_object_id_key UNIQUE (object_id),
	CONSTRAINT couriersystem_restaurants_pkey PRIMARY KEY (id)
);