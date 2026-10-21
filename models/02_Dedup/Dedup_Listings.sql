{{
    config(
        materialized = 'ephemeral'
    )
}}

{{dedup_and_deletion('Staging_Listings', 'LISTING_ID', 'LOADED_AT', 'TO_BE_DELETED')}}