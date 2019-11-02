
-- Drop table

-- DROP TABLE staging_tables.farpost;

CREATE TABLE staging_tables.farpost (
	id int4 NULL,
	title text NULL,
	"text" text NULL,
	clean_text text NULL,
	lem_text text NULL,
	image json NULL,
	address text NULL,
	status_house bool NULL,
	is_builder bool NULL,
	price int8 NULL,
	area float8 NULL,
	is_mortage bool NULL,
	floor text NULL,
	url text NULL,
	is_balcony bool NULL,
	"source" text NULL,
	load_date timestamp NULL
);
