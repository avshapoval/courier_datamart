-- stg.ordersystem_users definition

-- Drop table

-- DROP TABLE stg.ordersystem_users;

CREATE TABLE stg.ordersystem_users (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);