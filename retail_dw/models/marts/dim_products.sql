with stg_sales as (
    select * from {{ ref('stg_sales') }}
)
select distinct
    product_id,
    product_name
from stg_sales
where product_id is not null
  and product_name is not null
