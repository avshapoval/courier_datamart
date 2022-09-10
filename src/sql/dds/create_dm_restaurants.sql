-- dds.dm_restaurants definition

-- Drop table

-- DROP TABLE dds.dm_restaurants;

CREATE TABLE dds.dm_restaurants (
	id int4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);