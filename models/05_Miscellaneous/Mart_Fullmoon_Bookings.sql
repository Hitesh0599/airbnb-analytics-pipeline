SELECT  
    t3.*,
    CASE 
        WHEN t3.FULL_MOON_DATE is NOT NULL THEN 'Yes'
        ELSE 'No'
    END as Near_Full_Moon
FROM
    (
        SELECT  
            t1.BOOKING_ID,
            t1.LISTING_ID,
            t1.BOOKING_DATE,
            t1.NIGHTS_BOOKED,
            t1.BOOKING_AMOUNT,
            t1.CLEANING_FEE,
            t1.SERVICE_FEE,
            t1.BOOKING_STATUS,
            t1.BOOKING_LOADED_AT,
            t2.FULL_MOON_DATE
        FROM
            {{ref('Fct_Bookings')}} t1
        LEFT JOIN 
            {{ref('seed_full_moon_dates')}} t2
        on abs(datediff(DAY, t1.BOOKING_DATE, t2.FULL_MOON_DATE)) <= 3
    ) t3
