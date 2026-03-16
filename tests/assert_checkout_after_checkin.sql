-- Test fails if checkout date is on or before checkin date
-- Physically impossible booking dates indicate bad data

SELECT
    booking_id,
    check_in,
    check_out,
    nights
FROM {{ ref('stg_bookings') }}
WHERE check_out <= check_in

