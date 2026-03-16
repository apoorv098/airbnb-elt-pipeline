WITH source AS (

    SELECT * FROM {{ source('bronze', 'raw_bookings') }}

),

renamed AS (

    SELECT
        booking_id,
        listing_id,
        guest_id,
        host_id,
        TRY_CAST(check_in AS DATE)                AS check_in,
        TRY_CAST(check_out AS DATE)               AS check_out,
        TRY_CAST(nights AS NUMBER)                AS nights,
        TRY_CAST(total_price_usd AS NUMBER(10,2)) AS total_price_usd,
        LOWER(TRIM(booking_status))               AS booking_status,
        LOWER(TRIM(payment_method))               AS payment_method,
        TRY_CAST(created_at AS DATE)              AS created_at,
        TRY_CAST(ingestion_date AS DATE)          AS ingestion_date,
        CURRENT_TIMESTAMP()                       AS _loaded_at

    FROM source
    WHERE booking_id IS NOT NULL
      AND listing_id IS NOT NULL

)

SELECT * FROM renamed