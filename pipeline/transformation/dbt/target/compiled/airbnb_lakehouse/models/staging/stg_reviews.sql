-- models/staging/stg_reviews.sql



SELECT
    review_id,
    listing_id,
    reviewer_id,
    review_date,
    review_year,
    review_month
FROM iceberg.silver.reviews
WHERE review_id IS NOT NULL
  AND listing_id IS NOT NULL
  AND review_date IS NOT NULL