SELECT 
	e.country,
	e.year,
    e.cumulative_co2_mt,
	t.temperature_change_from_co2_degrees_c
FROM co2_project.fact_emissions AS e
JOIN co2_project.fact_climate_impact AS t
ON e.country = t.country
AND e.year = t.year
WHERE e.country = 'Brazil' 
AND e.year >= 2000
AND e.cumulative_co2_mt IS NOT NULL
ORDER BY country, year;