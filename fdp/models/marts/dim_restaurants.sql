-- select 
-- restaurant_id, 
-- restaurant_name, 
-- city, cuisine, 
-- rating, 
-- rating_count, 
-- cost_for_two
-- from {{ ref('stg_restaurants') }}
SELECT
    restaurant_id,
    restaurant_name,
    city,
    rating,
    rating_count,
    cost AS cost_for_two
FROM {{ ref('stg_restaurants') }}