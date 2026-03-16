{{
    config(
        materialized = 'table',
        database     = 'GOLD',
        schema       = 'AIRBNB'
    )
}}

WITH hosts AS (

    SELECT * FROM {{ ref('stg_hosts') }}

),

booking_metrics AS (

    SELECT
        host_id,
        COUNT(booking_id)         AS total_bookings,
        COUNT(DISTINCT listing_id) AS active_listings,
        SUM(total_price_usd)      AS total_revenue_usd,
        AVG(total_price_usd)      AS avg_booking_value,
        COUNT(CASE WHEN booking_status = 'cancelled'
              THEN 1 END)         AS cancelled_bookings,
        ROUND(
            COUNT(CASE WHEN booking_status = 'cancelled'
                  THEN 1 END) * 100.0 /
            NULLIF(COUNT(booking_id), 0), 2
        )                         AS cancellation_rate_pct
    FROM {{ ref('int_bookings_enriched') }}
    GROUP BY host_id

),

review_metrics AS (

    SELECT
        b.host_id,
        COUNT(r.review_id)        AS total_reviews,
        ROUND(AVG(r.rating), 2)   AS avg_rating,
        ROUND(AVG(r.cleanliness), 2)   AS avg_cleanliness,
        ROUND(AVG(r.communication), 2) AS avg_communication,
        ROUND(AVG(r.location), 2)      AS avg_location,
        ROUND(AVG(r.value), 2)         AS avg_value
    FROM {{ ref('stg_reviews') }}          r
    JOIN {{ ref('int_bookings_enriched') }} b
      ON r.booking_id = b.booking_id
    GROUP BY b.host_id

),

final AS (

    SELECT
        -- Host profile
        h.host_id,
        h.host_name,
        h.host_email,
        h.city,
        h.country,
        h.is_superhost,
        h.host_since,
        h.total_listings,
        h.response_rate,
        h.acceptance_rate,
        h.is_identity_verified,

        -- Booking performance
        COALESCE(b.total_bookings,        0) AS total_bookings,
        COALESCE(b.active_listings,       0) AS active_listings,
        COALESCE(b.total_revenue_usd,     0) AS total_revenue_usd,
        COALESCE(b.avg_booking_value,     0) AS avg_booking_value,
        COALESCE(b.cancelled_bookings,    0) AS cancelled_bookings,
        COALESCE(b.cancellation_rate_pct, 0) AS cancellation_rate_pct,

        -- Review performance
        COALESCE(r.total_reviews,         0) AS total_reviews,
        COALESCE(r.avg_rating,            0) AS avg_rating,
        COALESCE(r.avg_cleanliness,       0) AS avg_cleanliness,
        COALESCE(r.avg_communication,     0) AS avg_communication,
        COALESCE(r.avg_location,          0) AS avg_location,
        COALESCE(r.avg_value,             0) AS avg_value,

        -- Derived performance tier
        CASE
            WHEN COALESCE(r.avg_rating, 0) >= 4.8
             AND h.is_superhost = TRUE        THEN 'Elite'
            WHEN COALESCE(r.avg_rating, 0) >= 4.5 THEN 'Top rated'
            WHEN COALESCE(r.avg_rating, 0) >= 4.0 THEN 'Good'
            WHEN COALESCE(r.avg_rating, 0) >  0   THEN 'Needs improvement'
            ELSE                                        'No reviews yet'
        END                                  AS performance_tier,

        -- Metadata
        CURRENT_TIMESTAMP()                  AS _loaded_at

    FROM hosts               h
    LEFT JOIN booking_metrics b ON h.host_id = b.host_id
    LEFT JOIN review_metrics  r ON h.host_id = r.host_id

)

SELECT * FROM final