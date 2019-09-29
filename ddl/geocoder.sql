
-- Drop table

-- DROP TABLE staging_tables.geocoder;

CREATE TABLE staging_tables.geocoder (
	address text NULL,
	latitude float8 NULL,
	longitude float8 NULL,
	geom geometry(POINT, 4326) NULL
);
