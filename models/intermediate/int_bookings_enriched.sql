{{
    config(
        materialized         = 'incremental',
        unique_key           = 'booking_id',
        incremental_strategy = 'merge'
    )
}}

WITH bookings AS (

    SELECT * FROM {{ ref('stg_bookings') }}

    {% if is_incremental() %}
        WHERE ingestion_date > (
            SELECT MAX(ingestion_date) FROM {{ this }}
        )
    {% endif %}

),

listings AS (

    SELECT
        listing_id,
        city,
        neighbourhood,
        room_type,
        price_usd
    FROM {{ ref('stg_listings') }}

),

hosts AS (

    SELECT
        host_id,
        host_name,
        is_superhost
    FROM {{ ref('stg_hosts') }}

),

enriched AS (

    SELECT
        b.booking_id,
        b.listing_id,
        b.guest_id,
        b.host_id,
        h.host_name,
        h.is_superhost,
        l.city,
        l.neighbourhood,
        l.room_type,
        l.price_usd                              AS listing_price_usd,
        b.check_in,
        b.check_out,
        b.nights,
        b.total_price_usd,
        b.booking_status,
        b.payment_method,
        b.ingestion_date,
        CURRENT_TIMESTAMP()                      AS _loaded_at
    FROM bookings        b
    LEFT JOIN listings   l ON b.listing_id = l.listing_id
    LEFT JOIN hosts      h ON b.host_id    = h.host_id

)

SELECT * FROM enriched