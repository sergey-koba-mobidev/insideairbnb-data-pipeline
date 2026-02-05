{ { config(
  materialized = 'table',
  engine = 'MergeTree()',
  order_by = ['country', 'city', 'neighbourhood']
) } }
SELECT
  partition_country as country,
  partition_city as city,
  neighbourhood_group,
  neighbourhood,
  partition_date
FROM
  { { source_iceberg('iceberg', 'neighbourhoods') } }