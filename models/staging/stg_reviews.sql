WITH source AS (

    SELECT * FROM {{ source('bronze', 'raw_reviews') }}

),

renamed AS (

    SELECT
        review_id,
        booking_id,
        listing_id,
        reviewer_id,
        TRY_CAST(rating AS NUMBER(3,1))           AS rating,
        TRY_CAST(cleanliness AS NUMBER(3,1))      AS cleanliness,
        TRY_CAST(communication AS NUMBER(3,1))    AS communication,
        TRY_CAST(location AS NUMBER(3,1))         AS location,
        TRY_CAST(value AS NUMBER(3,1))            AS value,

        -- Derived column: average of all sub-ratings
        ROUND((
            CAST(TRY_CAST(cleanliness   AS NUMBER(3,1)) AS FLOAT) +
            CAST(TRY_CAST(communication AS NUMBER(3,1)) AS FLOAT) +
            CAST(TRY_CAST(location      AS NUMBER(3,1)) AS FLOAT) +
            CAST(TRY_CAST(value         AS NUMBER(3,1)) AS FLOAT)
        ) / 4, 2)                                 AS avg_sub_rating,

        TRIM(review_text)                         AS review_text,
        TRY_CAST(reviewed_at AS DATE)             AS reviewed_at,
        TRY_CAST(ingestion_date AS DATE)          AS ingestion_date,
        CURRENT_TIMESTAMP()                       AS _loaded_at

    FROM source
    WHERE review_id IS NOT NULL

)

SELECT * FROM renamed