select 
customer_id,
customer_name,
email,
age,
CASE WHEN age <25 then 'genZ'
WHEN age <40 then 'millenial'
WHEN age <55 then 'genx'
WHEN age is null then 'unknown'
else 'boomer' END as age_segment,
gender,
marital_status,
occupation,
income_band,
education,
family_size
from {{ref ('stg_users') }}