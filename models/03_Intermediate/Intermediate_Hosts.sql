SELECT 
    HOST_ID, 
    HOST_NAME,
    HOST_SINCE,
    IS_SUPERHOST,
    RESPONSE_RATE,
    LOADED_AT as HOSTS_LOADED_AT,
    DBT_SCD_ID,
    DBT_VALID_FROM,
    DBT_VALID_TO,
    {{dbt_utils.generate_surrogate_key(['HOST_ID', 'DBT_VALID_FROM'])}} as SURROGATE_HOST_ID
FROM
    {{ref('Snapshot_Hosts')}}