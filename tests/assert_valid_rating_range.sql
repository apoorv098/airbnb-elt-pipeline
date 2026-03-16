-- Test fails if any rating is outside the valid 1-5 range
-- Airbnb ratings are always between 1 and 5

SELECT
    review_id,
    listing_id,
    rating,
    cleanliness,
    communication,
    location,
    value
FROM {{ ref('stg_reviews') }}
WHERE rating        NOT BETWEEN 1 AND 5
   OR cleanliness   NOT BETWEEN 1 AND 5
   OR communication NOT BETWEEN 1 AND 5
   OR location      NOT BETWEEN 1 AND 5
   OR value         NOT BETWEEN 1 AND 5