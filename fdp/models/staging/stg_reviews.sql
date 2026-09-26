SELECT
    r.review_id,
    r.order_id,
    r.user_id::NUMBER AS customer_id,
    r.restaurant_id::STRING AS restaurant_id,
    r.rating::NUMBER AS rating,
    r.comment::STRING AS comment,
    r.review_date::DATE AS review_date,
    res.city AS city
FROM {{ source('raw', 'reviews') }} r
LEFT JOIN {{ ref('stg_restaurants') }} res
    ON r.restaurant_id = res.restaurant_id
WHERE r.comment IS NOT NULL