-- Test fails if any confirmed booking has zero or negative revenue
-- A confirmed booking must always have a positive amount

SELECT
    booking_id,
    total_price_usd,
    booking_status
FROM {{ ref('int_bookings_enriched') }}
WHERE booking_status != 'cancelled'
  AND total_price_usd <= 0