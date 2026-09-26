
-- parse the messy dimension (-- →null, 50+ ratings→50, ₹ 200→200, city after last comma):

-- SELECT
--     ID,
--     NAME,
--     CITY,
--     RATING,
--     RATING_COUNT,
--     COST,
--     LIC_NO,
--     LINK,
--     ADDRESS,
--     MENU
-- FROM {{ source('raw', 'restaurants') }}

-- Staging model for restaurant data
-- Standardize source column names and prepare data for downstream marts

SELECT
    ID AS restaurant_id,
    NAME AS restaurant_name,
    CITY AS city,
    RATING AS rating,
    RATING_COUNT AS rating_count,
    COST AS cost,
    LIC_NO AS license_no,
    LINK AS restaurant_link,
    ADDRESS AS address,
    MENU AS menu
FROM {{ source('raw', 'restaurants') }}