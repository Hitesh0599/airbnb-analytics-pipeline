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
    TRY_TO_DECIMAL(REPLACE(REPLACE(PRICE_PER_NIGHT, '$', ''), ',', ''), 12, 2) as PRICE_PER_NIGHT,
    LOADED_AT as LISTING_LOADED_AT,
    DBT_SCD_ID,
    DBT_VALID_FROM,
    DBT_VALID_TO,
    {{dbt_utils.generate_surrogate_key(['LISTING_ID', 'DBT_VALID_FROM'])}} as SURROGATE_LISTING_ID
FROM
    {{ref('Snapshot_Listings')}}


