WITH source AS (

    SELECT * FROM {{ source('bronze', 'raw_hosts') }}

),

renamed AS (

    SELECT
        host_id,
        INITCAP(TRIM(host_name))                  AS host_name,
        LOWER(TRIM(host_email))                   AS host_email,
        TRY_CAST(host_since AS DATE)              AS host_since,
        TRIM(city)                                AS city,
        TRIM(country)                             AS country,

        -- Convert string to boolean
        CASE WHEN LOWER(superhost) = 'true'
             THEN TRUE ELSE FALSE END             AS is_superhost,

        TRY_CAST(total_listings AS NUMBER)        AS total_listings,
        TRY_CAST(response_rate AS NUMBER(5,2))    AS response_rate,
        TRY_CAST(acceptance_rate AS NUMBER(5,2))  AS acceptance_rate,

        -- Convert string to boolean
        CASE WHEN LOWER(identity_verified) = 'true'
             THEN TRUE ELSE FALSE END             AS is_identity_verified,

        TRY_CAST(ingestion_date AS DATE)          AS ingestion_date,
        CURRENT_TIMESTAMP()                       AS _loaded_at

    FROM source
    WHERE host_id IS NOT NULL

)

SELECT * FROM renamed
