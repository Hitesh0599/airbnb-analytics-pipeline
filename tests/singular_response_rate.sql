SELECT
    *
FROM
    {{ref('Dedup_Hosts')}}
WHERE RESPONSE_RATE NOT BETWEEN 0 and 100