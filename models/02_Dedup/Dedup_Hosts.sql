{{
    config(
        materialized = 'ephemeral'
    )
}}

{{dedup_and_deletion('Staging_Hosts', 'HOST_ID', 'LOADED_AT', 'TO_BE_DELETED')}}