{{
    config(
        materialized = 'table',
        database     = 'GOLD',
        schema       = 'AIRBNB'
    )
}}

WITH listings AS (

    SELECT * FROM {{ ref('int_listings_with_metrics') }}

),

hosts AS (

    SELECT
        host_id,
        host_name,
        is_superhost,
        response_rate,
        acceptance_rate,
        host_since
    FROM {{ ref('stg_hosts') }}

),

final AS (

    SELECT
        -- Listing identifiers
        l.listing_id,
        l.host_id,
        h.host_name,
        h.is_superhost,
        h.response_rate,
        h.acceptance_rate,
        h.host_since,

        -- Location
        l.neighbourhood,
        l.city,
        l.country,
        l.room_type,

        -- Pricing
        l.price_usd,
        l.min_nights,
        l.max_nights,
        l.availability_365,

        -- Booking performance
        l.total_bookings,
        l.total_revenue_usd,
        l.avg_booking_value,
        ROUND(
            l.total_bookings /
            NULLIF(l.availability_365, 0) * 100, 2
        )                                AS occupancy_rate,

        -- Review performance
        l.total_reviews,
        l.avg_rating,
        l.avg_cleanliness,
        l.avg_communication,
        l.avg_location,
        l.avg_value,

        -- Metadata
        l.ingestion_date,
        CURRENT_TIMESTAMP()              AS _loaded_at

    FROM listings   l
    LEFT JOIN hosts h ON l.host_id = h.host_id

)

SELECT * FROM final