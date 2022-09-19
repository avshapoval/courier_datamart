CREATE TABLE IF NOT EXISTS  stg.srv_wf_settings (
	id serial4 NOT NULL,
	workflow_key varchar NULL,
	workflow_settings text NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id)
);