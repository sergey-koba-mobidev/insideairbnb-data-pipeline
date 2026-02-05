{ { config(
  materialized = 'table',
  engine = 'MergeTree()',
  order_by = ['country', 'city', 'neighbourhood']
) } }
SELECT
  partition_country as country,
  partition_city as city,
  neighbourhood,
  neighbourhood_group,
  -- Geometry is transferred as a string for ClickHouse compatibility
  geometry :: String as geometry,
  partition_date
FROM
  { { source_iceberg('iceberg', 'geo_neighbourhoods') } }