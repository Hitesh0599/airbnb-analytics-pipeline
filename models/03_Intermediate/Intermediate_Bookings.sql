WITH CTE_DE_DUPLICATION as(
    SELECT 
        t1.*
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY BOOKING_ID ORDER BY LOADED_AT DESC NULLS LAST) as rn
            FROM 
                {{ref('Staging_Bookings')}}
        ) as t1
    WHERE t1.rn = 1
) 

SELECT 
    BOOKING_ID,
    LISTING_ID,
    BOOKING_DATE,
    NIGHTS_BOOKED,
    try_to_decimal(replace(replace(BOOKING_AMOUNT, '$', ''), ',', ''), 12, 2) as BOOKING_AMOUNT,
    try_to_decimal(replace(replace(CLEANING_FEE, '$', ''), ',', ''), 12, 2) as CLEANING_FEE,
    try_to_decimal(replace(replace(SERVICE_FEE, '$', ''), ',', ''), 12, 2) as SERVICE_FEE,
    BOOKING_STATUS,
    LOADED_AT as BOOKING_LOADED_AT
FROM
    CTE_DE_DUPLICATION 
