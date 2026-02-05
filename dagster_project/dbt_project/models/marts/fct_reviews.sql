{{ config(
  materialized = 'incremental',
  engine = 'ReplacingMergeTree(updated_at)',
  order_by = ['id', 'partition_country', 'partition_city', 'partition_date'],
  unique_key = 'id'
) }}
SELECT
  id,
  listing_id,
  date,
  reviewer_id,
  reviewer_name,
  comments,
  partition_country,
  partition_city,
  partition_date,
  now() as updated_at
FROM
  {{ source_iceberg('iceberg', 'reviews') }} {% if is_incremental() %}
WHERE
  partition_date >= (
    SELECT
      max(partition_date)
    FROM
      {{ this }}
  ) {% endif %}