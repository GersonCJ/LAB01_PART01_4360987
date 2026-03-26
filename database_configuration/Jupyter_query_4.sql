SELECT 
	e.country,
	SUM(e.co2_per_capita_t_per_person + c.ghg_per_capita_t_per_person) AS cumulative_intensity
FROM co2_project.fact_emissions AS e
JOIN co2_project.fact_non_co2_ghg AS c
ON e.country = c.country
AND e.year = c.year
WHERE e.country IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Guyana', 'Paraguay', 'Peru', 'Suriname', 'Uruguay', 'Venezuela')
AND e.year >= 2001
GROUP BY e.country
ORDER BY cumulative_intensity ASC