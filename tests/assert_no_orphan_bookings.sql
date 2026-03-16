-- Test fails if any booking references a listing that doesn't exist
-- Every booking must have a valid parent listing

SELECT
    b.booking_id,
    b.listing_id,
    b.total_price_usd
FROM {{ ref('stg_bookings') }}       b
LEFT JOIN {{ ref('stg_listings') }}  l
       ON b.listing_id = l.listing_id
WHERE l.listing_id IS NULL