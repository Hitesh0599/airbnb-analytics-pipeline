{% docs Dedup_Hosts %}
    This is built upon Host model of Staging layer and to remove duplicacy we create this model with latest instance of each host_id taken. 
    Additionally, we applied deletion logic on this dimension by filtering out records with both host_name & response_rate = NULL.
    Then we create snapshot on this model and on not finding the records to be deleted, they would be removed from the snapshot.
    The grain is the unique hosts.  
{% enddocs %} 

{% docs Dedup_Listings %}
    This is built upon Listing model of Staging layer and to remove duplicacy we create this model with latest instance of each listing_id taken. 
    Additionally, we applied deletion logic on this dimension by filtering out records with both property_name & city = NULL.
    Then we create snapshot on this model and on not finding the records to be deleted, they would be removed from the snapshot.
    The grain is the unique listing.    
{% enddocs %} 

{% docs Snapshots_Hosts%}
    SCD Type 2 history of hosts, one row per host version. Built on Dedup_Hosts, which keeps the latest row per host and removes delete rows. Uses the timestamp strategy on LOADED_AT with HOST_ID as the unique key, so a new version is recorded whenever a host arrives with a later LOADED_AT. Hosts that disappear from Dedup_Hosts (deleted hosts) are closed out by setting dbt_valid_to (hard_deletes: invalidate). The current version of a host is the row where dbt_valid_to is null.
{% enddocs %}

{% docs Snapshots_Listings%}
    SCD Type 2 history of listings, one row per listing version. Built on Dedup_Listings, which keeps the latest row per listing and removes delete rows. Uses the timestamp strategy on LOADED_AT with LISTING_ID as the unique key, so a new version is recorded whenever a listing arrives with a later LOADED_AT. Listings that disappear from Dedup_Listings (deleted listings) are closed out by setting dbt_valid_to (hard_deletes: invalidate). The current version of a listing is the row where dbt_valid_to is null.
{% enddocs %}

{% docs scd2_date_handling %}
  Snapshot dates record when the pipeline first loaded a version, not when it began in the real world, so a booking can predate the first version of its listing or host. 
  To make every booking match exactly one version, the earliest version of each key is set to start at 1900-01-01, and the current version's DBT_VALID_TO (null in the snapshot) is set to 9999-12-31 23:59:59, 
  because comparisons with null never succeed. Join with booking_date >= DBT_VALID_FROM and booking_date < DBT_VALID_TO.
{% enddocs %}

{% docs Dasboard %}
  Airbnb Performance Dashboard tracking booking volume, revenue, and cancellation
  behaviour across 200 hosts and 500 listings (Dec 2024 to Dec 2025), with breakdowns
  by continent, room type, property type, and host type.
{% enddocs %}