CREATE TABLE co2_project.fact_non_co2_ghg(
	country VARCHAR(255),
	year INT,
	iso_code VARCHAR(30),
	population_people BIGINT,
    gdp_usd FLOAT8,
	ghg_excluding_lucf_per_capita_t_per_person FLOAT8,
    ghg_per_capita_t_per_person FLOAT8,
    methane_mt FLOAT8,
    methane_per_capita_t_per_person FLOAT8,
    nitrous_oxide_mt FLOAT8,
    nitrous_oxide_per_capita_t_per_person FLOAT8,
    total_ghg_mt FLOAT8,
    total_ghg_excluding_lucf_mt FLOAT8,
	PRIMARY KEY (year, iso_code)
)