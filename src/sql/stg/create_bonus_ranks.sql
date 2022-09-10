-- stg.bonussystem_ranks definition

-- Drop table

-- DROP TABLE stg.bonussystem_ranks;

CREATE TABLE stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
	min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0
);