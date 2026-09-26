SELECT
    f.restaurant_id,
    r.restaurant_name,
    r.city,
    COUNT(*) AS orders,
    SUM(IFF(f.is_delivered, f.sales_amount, 0)) AS revenue,
    ROUND(AVG(f.customer_rating), 2) AS avg_customer_rating,
    ROUND(AVG(f.delivery_time_min), 1) AS avg_delivery_min
FROM {{ ref('fct_orders') }} f
LEFT JOIN {{ ref('dim_restaurants') }} r
    USING (restaurant_id)
GROUP BY 1, 2, 3
