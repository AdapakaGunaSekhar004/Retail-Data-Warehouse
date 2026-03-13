with stg_sales as (
    select * from {{ ref('stg_sales') }}
)

select
    order_id,
    order_date,
    customer_id,
    customer_name,
    product_id,
    product_name,
    category,
    region,
    quantity,
    unit_price,
    total_amount
from stg_sales
