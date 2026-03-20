with stg_sales as (
    select * from {{ ref('stg_sales') }}
)
select
    order_id,
    order_date,
    order_time,
    customer_id,
    product_id,
    product_name,
    country,
    quantity,
    unit_price,
    total_amount
from stg_sales
