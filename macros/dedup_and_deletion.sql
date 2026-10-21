{% macro dedup_and_deletion(ref_model, key_column, order_by_column, deletion_flag) %}
with CTE_Ranked as (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY {{key_column}} ORDER BY {{order_by_column}} DESC) as rn
    FROM    
        {{ref(ref_model)}}
)

SELECT
    *
FROM 
    CTE_Ranked 
WHERE 
    rn = 1
AND 
    coalesce({{deletion_flag}}, FALSE) = FALSE
{% if this.name | trim == 'Dedup_Listings'%}
AND 
    HOST_ID in (SELECT distinct HOST_ID from {{ref('Dedup_Hosts')}})
{% endif %}

{% endmacro %}