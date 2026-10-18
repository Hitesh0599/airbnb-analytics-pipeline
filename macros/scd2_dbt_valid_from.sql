{% macro scd2_dbt_valid_from(validity_from_column, row_number_column) %}
    CASE
        WHEN {{row_number_column}} = 1 THEN '1900-01-01 00:00:00'::timestamp
        ELSE {{validity_from_column}}
    END as {{validity_from_column}}
{% endmacro %}