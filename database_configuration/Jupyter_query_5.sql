SELECT
    country,
    (SUM(nitrous_oxide_mt) / SUM(NULLIF(total_ghg_mt, 0))) * 100 AS hidden_impact
FROM co2_project.fact_non_co2_ghg
WHERE year >= 2001
  AND total_ghg_mt > 0 -- Security filter
GROUP BY country
ORDER BY hidden_impact DESC
LIMIT 5;