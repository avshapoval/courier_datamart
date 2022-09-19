CREATE TABLE IF NOT EXISTS dds.dm_orders(
	id serial4 PRIMARY KEY ,
	order_key varchar,
	order_ts timestamp,
	restaurant_id int,
	"sum" numeric(14, 2)
);