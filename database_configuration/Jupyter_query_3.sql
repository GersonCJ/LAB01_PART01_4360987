SELECT 
	es.country,
	es.year,
	es.land_use_change_co2_mt,
	c.total_ghg_mt,
	(es.land_use_change_co2_mt / c.total_ghg_mt) * 100 AS land_use_over_ghg_prct
FROM co2_project.fact_emission_sources AS es
JOIN co2_project.fact_non_co2_ghg AS c
ON es.country = c.country
AND es.year = c.year
WHERE es.country = 'Brazil'
AND es.year >= 1990