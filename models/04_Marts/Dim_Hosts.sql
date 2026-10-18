WITH CTE_RANKING_HOST_IDS as (
    SELECT 
        SURROGATE_HOST_ID,
        HOST_ID, 
        HOST_NAME,
        HOST_SINCE,
        IS_SUPERHOST,
        RESPONSE_RATE,
        DBT_VALID_FROM,
        DBT_VALID_TO,
        ROW_NUMBER() OVER (PARTITION BY HOST_ID ORDER BY HOSTS_LOADED_AT asc) as rn
    FROM
        {{ref('Intermediate_Hosts')}}
)
SELECT 
    SURROGATE_HOST_ID,
    HOST_ID, 
    HOST_NAME,
    HOST_SINCE,
    IS_SUPERHOST,
    RESPONSE_RATE,
    {{scd2_dbt_valid_from('DBT_VALID_FROM', 'rn')}},
    {{scd2_dbt_valid_to('DBT_VALID_TO')}} 
FROM
    CTE_RANKING_HOST_IDS
