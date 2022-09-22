CREATE TABLE IF NOT EXISTS 	cdm.dm_courier_ledger(
	id serial4 PRIMARY KEY,
	courier_id int NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year SMALLINT NOT NULL CHECK (settlement_year > 1980 AND settlement_year < 2500),
	settlement_month SMALLINT NOT NULL CHECK (settlement_month >= 1 AND settlement_month <= 12),
	orders_count int NOT NULL CHECK (orders_count >= 0),
	orders_total_sum numeric(14, 2) NOT NULL CHECK (orders_total_sum >= 0),
	rate_avg numeric(3, 2) NOT NULL CHECK (rate_avg >= 0),
	order_processing_fee numeric(14, 2) NOT NULL,
	courier_order_sum numeric(14, 2) NOT NULL CHECK (courier_order_sum >= 0),
	courier_tips_sum numeric(14, 2) NOT NULL CHECK (courier_tips_sum >= 0),
	courier_reward_sum numeric(14, 2) NOT NULL	
);