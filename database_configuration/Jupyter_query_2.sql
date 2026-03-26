SELECT
	e.country,
	e.year,
	e.co2_mt,
	c.consumption_co2_mt,
	-- The Leakage Calculation
	(c.consumption_co2_mt - e.co2_mt) AS carbon_gap
FROM co2_project.agg_emissions AS e
JOIN co2_project.agg_consumption AS c
ON e.country = c.country
AND e.year = c.year
WHERE e.country = 'High-income countries'
AND e.year >= 1990