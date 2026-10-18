{% macro scd2_dbt_valid_to(validity_to_column) %}
    COALESCE({{validity_to_column}}, '9999-12-31 23:59:59'::timestamp) as {{validity_to_column}}
{% endmacro %}
