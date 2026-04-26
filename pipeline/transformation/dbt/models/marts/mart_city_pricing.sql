-- models/marts/mart_city_pricing.sql
-- Gold table: phân tích giá theo city và room type
-- Đây là table Superset sẽ query để vẽ dashboard

{{ config(
    materialized='table',
    properties={
      "format": "'PARQUET'",
      "partitioning": "ARRAY['city']"
    }
) }}

WITH base AS (
    SELECT
        city,
        room_type,
        property_type,
        price,
        bedrooms,
        accommodates,
        amenity_count,
        review_scores_rating,
        review_scores_cleanliness,
        review_scores_location,
        review_scores_value,
        host_is_superhost,
        instant_bookable,
        listing_id
    FROM {{ ref('stg_listings') }}
    WHERE city IS NOT NULL
),

city_room_stats AS (
    SELECT
        city,
        room_type,

        -- Volume
        COUNT(listing_id)                           AS total_listings,
        COUNT(DISTINCT property_type)               AS distinct_property_types,

        -- Pricing
        ROUND(AVG(price), 2)                        AS avg_price,
        ROUND(APPROX_PERCENTILE(price, 0.5), 2)     AS median_price,
        ROUND(MIN(price), 2)                        AS min_price,
        ROUND(MAX(price), 2)                        AS max_price,
        ROUND(STDDEV(price), 2)                     AS stddev_price,

        -- Capacity
        ROUND(AVG(bedrooms), 1)                     AS avg_bedrooms,
        ROUND(AVG(accommodates), 1)                 AS avg_accommodates,
        ROUND(AVG(amenity_count), 1)                AS avg_amenities,

        -- Quality scores
        ROUND(AVG(review_scores_rating), 2)         AS avg_rating,
        ROUND(AVG(review_scores_cleanliness), 2)    AS avg_cleanliness,
        ROUND(AVG(review_scores_location), 2)       AS avg_location_score,
        ROUND(AVG(review_scores_value), 2)          AS avg_value_score,

        -- Host attributes
        ROUND(
            100.0 * SUM(CASE WHEN host_is_superhost THEN 1 ELSE 0 END)
            / COUNT(*), 1
        )                                           AS superhost_pct,
        ROUND(
            100.0 * SUM(CASE WHEN instant_bookable THEN 1 ELSE 0 END)
            / COUNT(*), 1
        )                                           AS instant_bookable_pct

    FROM base
    GROUP BY city, room_type
)

SELECT
    *,
    -- Price tier label (dùng cho filter trên dashboard)
    CASE
        WHEN median_price < 50   THEN 'Budget'
        WHEN median_price < 100  THEN 'Mid-range'
        WHEN median_price < 200  THEN 'Premium'
        ELSE 'Luxury'
    END AS price_tier,
    CURRENT_TIMESTAMP AS updated_at
FROM city_room_stats
ORDER BY city, room_type
