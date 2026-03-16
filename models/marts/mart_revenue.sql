{{
    config(
        materialized = 'table',
        database     = 'GOLD',
        schema       = 'AIRBNB'
    )
}}

WITH bookings AS (

    SELECT * FROM {{ ref('int_bookings_enriched') }}

),

listings AS (

    SELECT
        listing_id,
        country
    FROM {{ ref('stg_listings') }}

),

final AS (

    SELECT
        -- Dimensions
        b.city,
        l.country,
        b.room_type,
        TO_CHAR(b.check_in, 'YYYY-MM')               AS booking_month,

        -- Volume metrics
        COUNT(b.booking_id)                           AS total_bookings,
        COUNT(CASE WHEN b.booking_status = 'confirmed'
              THEN 1 END)                             AS confirmed_bookings,
        COUNT(CASE WHEN b.booking_status = 'cancelled'
              THEN 1 END)                             AS cancelled_bookings,
        COUNT(CASE WHEN b.booking_status = 'completed'
              THEN 1 END)                             AS completed_bookings,

        -- Revenue metrics (exclude cancelled)
        SUM(CASE WHEN b.booking_status != 'cancelled'
            THEN b.total_price_usd ELSE 0 END)        AS total_revenue_usd,
        ROUND(AVG(
            CASE WHEN b.booking_status != 'cancelled'
            THEN b.total_price_usd END), 2)            AS avg_booking_value,

        -- Cancellation rate
        ROUND(
            COUNT(CASE WHEN b.booking_status = 'cancelled'
                  THEN 1 END) * 100.0 /
            NULLIF(COUNT(b.booking_id), 0), 2
        )                                              AS cancellation_rate_pct,

        -- Metadata
        CURRENT_TIMESTAMP()                           AS _loaded_at

    FROM bookings        b
    JOIN listings        l ON b.listing_id = l.listing_id
    GROUP BY
        b.city,
        l.country,
        b.room_type,
        TO_CHAR(b.check_in, 'YYYY-MM')

)

SELECT * FROM final