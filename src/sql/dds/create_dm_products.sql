-- dds.dm_products definition

-- Drop table

-- DROP TABLE dds.dm_products;

CREATE TABLE dds.dm_products (
	id int4 NOT NULL,
	restaurant_id int4 NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) NOT NULL DEFAULT 0,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT check_product_price CHECK ((product_price >= (0)::numeric)),
	CONSTRAINT dm_products_pkey PRIMARY KEY (id)
);


-- dds.dm_products foreign keys

ALTER TABLE dds.dm_products ADD CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id) ON DELETE CASCADE;