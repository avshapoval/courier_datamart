-- dds.dm_orders definition

-- Drop table

-- DROP TABLE dds.dm_orders;

CREATE TABLE dds.dm_orders (
	id serial4 NOT NULL,
	user_id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	timestamp_id int4 NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_order_key_key UNIQUE (order_key),
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id)
);


-- dds.dm_orders foreign keys

ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_restaurant_id_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id) ON DELETE CASCADE;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_timestamp_id_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id) ON DELETE CASCADE;
ALTER TABLE dds.dm_orders ADD CONSTRAINT dm_orders_user_id_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id);