CREATE TABLE co2_project.fact_climate_impact(
	country VARCHAR(255),
	year INT,
	iso_code VARCHAR(30),
	population_people BIGINT,
    share_of_temperature_change_from_ghg_prct FLOAT8,
    temperature_change_from_ch4_degrees_c FLOAT8,
    temperature_change_from_co2_degrees_c FLOAT8,
    temperature_change_from_ghg_degrees_c FLOAT8,
    temperature_change_from_n2o_degrees_c FLOAT8,
	PRIMARY KEY (year, iso_code)
)