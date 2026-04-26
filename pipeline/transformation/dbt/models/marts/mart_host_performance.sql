-- models/marts/mart_host_performance.sql
-- Gold table: phân tích hiệu suất host

{{ config(materialized='table') }}

WITH listing_review_counts AS (
    SELECT
        listing_id,
        COUNT(review_id)    AS total_reviews,
        MIN(review_date)    AS first_review_date,
        MAX(review_date)    AS last_review_date
    FROM {{ ref('stg_reviews') }}
    GROUP BY listing_id
),

host_stats AS (
    SELECT
        l.host_id,
        l.city,
        l.host_is_superhost,
        l.host_response_rate_pct,
        l.host_acceptance_rate_pct,
        l.host_total_listings_count,
        l.host_identity_verified,

        COUNT(l.listing_id)                          AS listings_count,
        ROUND(AVG(l.price), 2)                       AS avg_listing_price,
        ROUND(AVG(l.review_scores_rating), 2)        AS avg_rating,
        ROUND(AVG(l.review_scores_communication), 2) AS avg_communication_score,
        SUM(COALESCE(r.total_reviews, 0))            AS total_reviews_received,
        ROUND(AVG(COALESCE(r.total_reviews, 0)), 1)  AS avg_reviews_per_listing,

        -- Tính "listing age" tính bằng ngày
        DATE_DIFF('day', MIN(r.first_review_date), MAX(r.last_review_date)) AS active_days

    FROM {{ ref('stg_listings') }} l
    LEFT JOIN listing_review_counts r ON l.listing_id = r.listing_id
    GROUP BY
        l.host_id, l.city, l.host_is_superhost,
        l.host_response_rate_pct, l.host_acceptance_rate_pct,
        l.host_total_listings_count, l.host_identity_verified
)

SELECT
    *,
    -- Phân loại host
    CASE
        WHEN host_is_superhost AND avg_rating >= 4.8  THEN 'Elite'
        WHEN host_is_superhost                        THEN 'Superhost'
        WHEN avg_rating >= 4.5                        THEN 'Experienced'
        ELSE 'Standard'
    END AS host_tier,
    CURRENT_TIMESTAMP AS updated_at
FROM host_stats
ORDER BY total_reviews_received DESC
