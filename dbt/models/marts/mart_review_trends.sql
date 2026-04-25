-- models/marts/mart_review_trends.sql
-- Gold table: review trends theo thời gian và thành phố

{{ config(materialized='table') }}

WITH monthly_reviews AS (
    SELECT
        r.review_year,
        r.review_month,
        l.city,
        l.room_type,

        COUNT(r.review_id)                          AS review_count,
        COUNT(DISTINCT r.listing_id)                AS active_listings,
        COUNT(DISTINCT r.reviewer_id)               AS unique_reviewers,
        ROUND(AVG(l.review_scores_rating), 2)       AS avg_rating,
        ROUND(AVG(l.review_scores_cleanliness), 2)  AS avg_cleanliness,
        ROUND(AVG(l.review_scores_value), 2)        AS avg_value,
        ROUND(AVG(l.price), 2)                      AS avg_price

    FROM {{ ref('stg_reviews') }} r
    JOIN {{ ref('stg_listings') }} l ON r.listing_id = l.listing_id
    WHERE r.review_year >= 2015     -- lọc bỏ data cũ quá
    GROUP BY r.review_year, r.review_month, l.city, l.room_type
)

SELECT
    *,
    -- Tạo date key cho time series chart
    DATE(CAST(review_year AS VARCHAR) || '-'
         || LPAD(CAST(review_month AS VARCHAR), 2, '0') || '-01') AS month_start_date,

    -- Review growth MoM (so sánh với tháng trước trong cùng city+room_type)
    LAG(review_count) OVER (
        PARTITION BY city, room_type
        ORDER BY review_year, review_month
    ) AS prev_month_reviews,

    ROUND(
        100.0 * (review_count - LAG(review_count) OVER (
            PARTITION BY city, room_type
            ORDER BY review_year, review_month
        )) / NULLIF(LAG(review_count) OVER (
            PARTITION BY city, room_type
            ORDER BY review_year, review_month
        ), 0), 1
    ) AS review_growth_pct,

    CURRENT_TIMESTAMP AS updated_at
FROM monthly_reviews
ORDER BY city, review_year, review_month
