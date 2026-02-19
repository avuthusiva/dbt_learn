select 
    to_date(r.created_at) created_at,
    p.id,
    p.product_name,
    avg(r.rating) as daily_rating
from {{ ref('bronze_reviews') }} r left join
{{ ref('silver_products') }} p
on (p.id = r.product_id)
group by all