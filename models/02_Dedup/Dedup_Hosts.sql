{#
{{
    config(
        materialized = 'ephemeral'
    )
}}

WITH CTE_DE_DUPLICATION as(
    SELECT  
        *, 
        ROW_NUMBER() OVER (PARTITION BY HOST_ID ORDER BY LOADED_AT DESC NULLS LAST) as rn
    FROM
        {{ref('Staging_Hosts')}}
)

SELECT
    HOST_ID, 
    HOST_NAME,
    HOST_SINCE,
    IS_SUPERHOST,
    RESPONSE_RATE,
    LOADED_AT 
FROM
    CTE_DE_DUPLICATION
WHERE 
    rn = 1
AND 
    (
        HOST_NAME IS NOT NULL 
        OR 
        RESPONSE_RATE IS NOT NULL
    )
#}


{{dedup_and_deletion('Staging_Hosts', 'HOST_ID', 'LOADED_AT', ['HOST_NAME', 'RESPONSE_RATE'])}}