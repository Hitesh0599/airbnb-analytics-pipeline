{% macro dedup_and_deletion(model_name, key_column, order_by_column, Filtering_Columns) %}
with CTE_Ranked as (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY {{key_column}} ORDER BY {{order_by_column}} DESC) as rn
    FROM    
        {{ref(model_name)}}
)

SELECT
    *
FROM 
    CTE_Ranked 
WHERE 
    rn = 1
AND 
    (
        {% for col in Filtering_Columns %}
            {{col}} is NOT NULL
                {% if not loop.last %}
                    OR 
                {% endif %}
        {% endfor %}
    )

{% endmacro %}