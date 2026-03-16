WITH source AS (

    SELECT * FROM {{ source('bronze', 'raw_listings') }}

),

renamed AS (

    SELECT
        listing_id,
        host_id,
        TRIM(neighbourhood)                       AS neighbourhood,
        TRIM(city)                                AS city,
        TRIM(country)                             AS country,
        TRIM(room_type)                           AS room_type,
        TRY_CAST(price_usd AS NUMBER(10,2))       AS price_usd,
        TRY_CAST(min_nights AS NUMBER)            AS min_nights,
        TRY_CAST(max_nights AS NUMBER)            AS max_nights,
        TRY_CAST(availability_365 AS NUMBER)      AS availability_365,
        TRY_CAST(rating AS NUMBER(4,2))           AS rating,
        TRY_CAST(review_count AS NUMBER)          AS review_count,
        TRY_CAST(created_at AS DATE)              AS created_at,
        TRY_CAST(ingestion_date AS DATE)          AS ingestion_date,
        CURRENT_TIMESTAMP()                       AS _loaded_at

    FROM source
    WHERE listing_id IS NOT NULL
      AND host_id    IS NOT NULL

)

SELECT * FROM renamed