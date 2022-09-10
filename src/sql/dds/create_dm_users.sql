-- dds.dm_users definition

-- Drop table

-- DROP TABLE dds.dm_users;

CREATE TABLE dds.dm_users (
	id serial4 NOT NULL,
	user_id varchar NOT NULL,
	user_name varchar NOT NULL,
	user_login varchar NOT NULL,
	CONSTRAINT dm_users_pk PRIMARY KEY (id)
);