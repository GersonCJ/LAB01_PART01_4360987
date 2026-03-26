SELECT 
    cols.column_name, 
    cols.data_type, 
    col_description((cols.table_schema || '.' || cols.table_name)::regclass, cols.ordinal_position) AS column_description
FROM information_schema.columns cols
WHERE cols.table_schema = 'co2_project' 
  AND cols.table_name = 'fact_emission_sources' -- Change to your other table names to check them too
ORDER BY cols.ordinal_position;