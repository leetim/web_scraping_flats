
-- Drop table

-- DROP TABLE staging_tables.house;

CREATE TABLE staging_tables.house (
	houseguid text NULL,
	address text NULL,
	built_year float8 NULL,
	floor_count_max float8 NULL,
	floor_count_min float8 NULL,
	entrance_count float8 NULL,
	elevators_count float8 NULL,
	living_quarters_count float8 NULL,
	area_residential text NULL,
	chute_count float8 NULL,
	parking_square text NULL,
	wall_material text NULL,
	load_date timestamp NULL
);
