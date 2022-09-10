-- dds.fct_product_sales definition

-- Drop table

-- DROP TABLE dds.fct_product_sales;

CREATE TABLE dds.fct_product_sales (
	id serial4 NOT NULL,
	product_id int4 NOT NULL,
	order_id int4 NOT NULL,
	count int4 NOT NULL DEFAULT 0,
	price numeric(14, 2) NOT NULL DEFAULT 0,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pkey PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric))
);


-- dds.fct_product_sales foreign keys

ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id) ON DELETE CASCADE;
ALTER TABLE dds.fct_product_sales ADD CONSTRAINT fct_product_sales_product_id_fk FOREIGN KEY (product_id) REFERENCES dds.dm_products(id) ON DELETE CASCADE;