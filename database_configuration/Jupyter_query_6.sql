SELECT 
	year,
    coal_co2_mt,
	gas_co2_mt
FROM co2_project.fact_emission_sources
WHERE country = 'Chile' AND year >= 1990;