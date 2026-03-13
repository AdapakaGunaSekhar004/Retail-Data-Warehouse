with stg_sales as (
    select * from {{ ref('stg_sales') }}
)

select distinct
    product_id,
    product_name,
    category
from stg_sales
