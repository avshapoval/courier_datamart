-- stg.bonussystem_events definition

-- Drop table

-- DROP TABLE stg.bonussystem_events;

CREATE TABLE stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL
);
CREATE INDEX idx_outbox__event_ts ON stg.bonussystem_events USING btree (event_ts);