{{
    config(
        materialized         = 'incremental',
        unique_key           = 'listing_id',
        incremental_strategy = 'merge'
    )
}}

WITH listings AS (

    SELECT * FROM {{ ref('stg_listings') }}

    {% if is_incremental() %}
        WHERE ingestion_date > (
            SELECT MAX(ingestion_date) FROM {{ this }}
        )
    {% endif %}

),

booking_metrics AS (

    SELECT
        listing_id,
        COUNT(booking_id)        AS total_bookings,
        SUM(total_price_usd)     AS total_revenue_usd,
        AVG(total_price_usd)     AS avg_booking_value
    FROM {{ ref('stg_bookings') }}
    WHERE booking_status != 'cancelled'
    GROUP BY listing_id

),

review_metrics AS (

    SELECT
        listing_id,
        COUNT(review_id)         AS total_reviews,
        AVG(rating)              AS avg_rating,
        AVG(cleanliness)         AS avg_cleanliness,
        AVG(communication)       AS avg_communication,
        AVG(location)            AS avg_location,
        AVG(value)               AS avg_value
    FROM {{ ref('stg_reviews') }}
    GROUP BY listing_id

),

final AS (

    SELECT
        l.listing_id,
        l.host_id,
        l.neighbourhood,
        l.city,
        l.country,
        l.room_type,
        l.price_usd,
        l.min_nights,
        l.max_nights,
        l.availability_365,

        -- Booking metrics (0 if no bookings)
        COALESCE(b.total_bookings,    0) AS total_bookings,
        COALESCE(b.total_revenue_usd, 0) AS total_revenue_usd,
        COALESCE(b.avg_booking_value, 0) AS avg_booking_value,

        -- Review metrics (use listing rating if no reviews yet)
        COALESCE(r.total_reviews,     0) AS total_reviews,
        COALESCE(r.avg_rating, l.rating) AS avg_rating,
        COALESCE(r.avg_cleanliness,   0) AS avg_cleanliness,
        COALESCE(r.avg_communication, 0) AS avg_communication,
        COALESCE(r.avg_location,      0) AS avg_location,
        COALESCE(r.avg_value,         0) AS avg_value,

        l.ingestion_date,
        CURRENT_TIMESTAMP()              AS _loaded_at

    FROM listings            l
    LEFT JOIN booking_metrics b ON l.listing_id = b.listing_id
    LEFT JOIN review_metrics  r ON l.listing_id = r.listing_id

)

SELECT * FROM final