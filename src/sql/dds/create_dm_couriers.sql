CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id int4 NOT NULL,
	object_id varchar NULL,
	"name" varchar NULL,
	CONSTRAINT dm_couriers_object_id_unique UNIQUE (object_id),
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);