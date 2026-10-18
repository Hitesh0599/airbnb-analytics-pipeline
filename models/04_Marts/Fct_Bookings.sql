{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'BOOKING_ID',
        on_schema_change = 'append_new_columns'
)
}}

WITH CTE_INCREMENTAL as (
    SELECT
        BOOKING_ID,
        LISTING_ID,
        BOOKING_DATE,
        NIGHTS_BOOKED,
        BOOKING_AMOUNT,
        CLEANING_FEE,
        SERVICE_FEE,
        BOOKING_STATUS,
        BOOKING_LOADED_AT
    FROM
        {{ref('Intermediate_Bookings')}}
    {% if is_incremental() %}
    WHERE 
        BOOKING_LOADED_AT > ( SELECT MAX(BOOKING_LOADED_AT) FROM {{this}} )
    or
        BOOKING_ID IN (SELECT BOOKING_ID FROM {{this}} WHERE SURROGATE_LISTING_ID is NULL OR SURROGATE_HOST_ID is NULL)
    {% endif %}    
)

SELECT
    booking.BOOKING_ID,
    booking.LISTING_ID,
    booking.BOOKING_DATE,
    booking.NIGHTS_BOOKED,
    booking.BOOKING_AMOUNT,
    booking.CLEANING_FEE,
    booking.SERVICE_FEE,
    booking.BOOKING_STATUS,
    booking.BOOKING_LOADED_AT,
    DL.SURROGATE_LISTING_ID,
    DL.HOST_ID,
    DH.SURROGATE_HOST_ID
FROM
    CTE_INCREMENTAL as booking
LEFT JOIN 
    {{ref('Dim_Listings')}} as DL
ON booking.LISTING_ID = DL.LISTING_ID and booking.BOOKING_DATE >= DL.DBT_VALID_FROM and booking.BOOKING_DATE < DL.DBT_VALID_TO
LEFT JOIN
    {{ref('Dim_Hosts')}} as DH
ON DL.HOST_ID = DH.HOST_ID and booking.BOOKING_DATE >= DH.DBT_VALID_FROM and booking.BOOKING_DATE < DH.DBT_VALID_TO