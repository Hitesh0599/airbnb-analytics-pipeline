WITH CTE_RANKING_LISTING_IDS as (
    SELECT 
        SURROGATE_LISTING_ID,
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
        DBT_VALID_FROM,
        DBT_VALID_TO,
        ROW_NUMBER() OVER (PARTITION BY LISTING_ID ORDER BY LISTING_LOADED_AT asc) as rn
    FROM
        {{ref('Intermediate_Listings')}}
)

SELECT 
    cte1.SURROGATE_LISTING_ID,
    cte1.LISTING_ID,
    cte1.HOST_ID,
    cte1.PROPERTY_TYPE,
    cte1.ROOM_TYPE,
    cte1.CITY,
    cte1.COUNTRY,
    seed.Continent_Name as CONTINENT,
    cte1.ACCOMMODATES,
    cte1.BEDROOMS,
    cte1.BATHROOMS,
    cte1.PRICE_PER_NIGHT,
    {{scd2_dbt_valid_from('DBT_VALID_FROM', 'rn')}},
    {{scd2_dbt_valid_to('DBT_VALID_TO')}} 
FROM
    CTE_RANKING_LISTING_IDS as cte1
LEFT JOIN
    {{ref('seed_country_continent')}} as seed
ON cte1.COUNTRY = seed.Country_Name