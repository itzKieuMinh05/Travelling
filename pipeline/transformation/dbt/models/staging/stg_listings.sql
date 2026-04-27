-- models/staging/stg_listings.sql
-- View nhẹ ngồi trên Silver Iceberg table

{{ config(materialized='table') }}

SELECT
    listing_id,
    name,
    host_id,
    host_since,
    host_is_superhost,
    host_response_rate_pct,
    host_acceptance_rate_pct,
    host_total_listings_count,
    host_identity_verified,
    neighbourhood,
    city,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bedrooms,
    amenities,
    amenity_count,
    price,
    minimum_nights,
    maximum_nights,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    instant_bookable,
    ingested_at
FROM iceberg.silver.listings
WHERE listing_id IS NOT NULL
  AND price > 0
