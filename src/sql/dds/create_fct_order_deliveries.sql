CREATE TABLE dds.fct_order_deliveries (
	id serial4,
	delivery_key varchar NULL,
	order_id int4 NULL,
	courier_id int4 NULL,
	delivery_ts timestamp NULL,
	tip_sum numeric(14, 2) NULL,
	rating numeric(2, 1) NULL,
	address varchar NULL,
	restaurant_id int4 NULL,
	CONSTRAINT fct_order_deliveries_delivery_key_unique UNIQUE (delivery_key),
	CONSTRAINT fct_order_deliviries_pkey PRIMARY KEY (id)
);


-- dds.fct_order_deliveries foreign keys

ALTER TABLE dds.fct_order_deliveries ADD CONSTRAINT fct_order_deliviries FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id);
ALTER TABLE dds.fct_order_deliveries ADD CONSTRAINT fct_order_deliviries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.fct_order_deliveries ADD CONSTRAINT fct_order_deliviries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);