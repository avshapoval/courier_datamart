CREATE TABLE dds.dm_restaurants (
	id int4 NOT NULL,
	object_id varchar NULL,
	"name" varchar NULL,
	CONSTRAINT dm_restaurants_object_id_unique UNIQUE (object_id),
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);