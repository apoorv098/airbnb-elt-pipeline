{% snapshot snap_hosts %}

{{
    config(
        target_database = 'SILVER',
        target_schema   = 'AIRBNB',
        unique_key      = 'host_id',
        strategy        = 'check',
        check_cols      = ['superhost', 'response_rate', 'acceptance_rate', 'total_listings']
    )
}}

SELECT
    host_id,
    host_name,
    host_email,
    host_since,
    city,
    country,
    superhost,
    total_listings,
    response_rate,
    acceptance_rate,
    identity_verified,
    ingestion_date
FROM {{ source('bronze', 'raw_hosts') }}

{% endsnapshot %}