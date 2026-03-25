CREATE TABLE co2_project.fact_consumption(
	country VARCHAR(255),
	year INT,
	iso_code VARCHAR(30),
	population_people BIGINT,
    gdp_usd FLOAT8,
	consumption_co2_mt FLOAT8,
    consumption_co2_per_capita_t_per_person FLOAT8,
    consumption_co2_per_gdp_kg_per_usd FLOAT8,
    energy_per_capita_kwh FLOAT8,
    energy_per_gdp_kwh FLOAT8,
    primary_energy_consumption_twh FLOAT8,
	PRIMARY KEY (year, iso_code)
)