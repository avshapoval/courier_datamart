CREATE TABLE dds.dm_orders (
	id serial4 NOT NULL,
	order_key varchar NULL,
	order_ts timestamp NULL,
	sum numeric(14, 2) NULL,
	CONSTRAINT dm_orders_order_key_unique UNIQUE (order_key),
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id)
);