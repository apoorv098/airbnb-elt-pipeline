{% snapshot snap_listings %}

{{
    config(
        target_database = 'SILVER',
        target_schema   = 'AIRBNB',
        unique_key      = 'listing_id',
        strategy        = 'check',
        check_cols      = ['price_usd', 'room_type', 'availability_365', 'rating']
    )
}}

SELECT
    listing_id,
    host_id,
    neighbourhood,
    city,
    country,
    room_type,
    price_usd,
    min_nights,
    max_nights,
    availability_365,
    rating,
    review_count,
    ingestion_date
FROM {{ source('bronze', 'raw_listings') }}

{% endsnapshot %}