{% macro source_iceberg(source_name, table_name) %}
    {# 
       The DataLakeCatalog database "silver" contains tables with names like "silver.listings"
       because "silver" is the Iceberg namespace.
    #}
    silver.`silver.{{ table_name }}`
{% endmacro %}
