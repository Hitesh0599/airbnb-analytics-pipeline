{#
{{
    config(
        materialized = 'ephemeral'
    )
}}

WITH CTE_DE_DUPLICATION as (
    SELECT
        *, 
        ROW_NUMBER() OVER (PARTITION BY HOST_ID ORDER BY LOADED_AT DESC NULLS LAST) as rn
    FROM
        {{ref('Staging_Listings')}}
)

SELECT  
    LISTING_ID,
    HOST_ID,
    PROPERTY_TYPE,
    ROOM_TYPE,
    CITY,
    COUNTRY,
    ACCOMMODATES,
    BEDROOMS,
    BATHROOMS,
    PRICE_PER_NIGHT,
    LOADED_AT 
FROM
    CTE_DE_DUPLICATION
WHERE 
    rn = 1
AND 
    (
        PROPERTY_TYPE IS NOT NULL 
        OR 
        CITY IS NOT NULL
    )
#}


{{dedup_and_deletion('Staging_Listings', 'LISTING_ID', 'LOADED_AT', ['PROPERTY_TYPE', 'CITY'])}}