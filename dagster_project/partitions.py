from dagster import DynamicPartitionsDefinition

airbnb_partitions = DynamicPartitionsDefinition(name="airbnb_city_date")
