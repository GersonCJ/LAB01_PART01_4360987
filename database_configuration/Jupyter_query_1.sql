SELECT country, 
	   year, 
	   iso_code, 
	   gdp_usd / 1e9 AS gdp_bilions, 
	   population_people, 
	   total_ghg_mt, 
	   total_ghg_mt / NULLIF(gdp_usd / 1e9, 0) AS ghg_intensity 
FROM co2_project.fact_non_co2_ghg 
WHERE country = 'Brazil' AND year >= 1996;