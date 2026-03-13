with source as (
    select * from raw_sales
),

renamed as (
    select
        order_id,
        order_date::date          as order_date,
        customer_id,
        customer_name,
        product_id,
        product_name,
        category,
        quantity::integer         as quantity,
        unit_price::numeric(10,2) as unit_price,
        region,
        (quantity::integer * unit_price::numeric(10,2)) as total_amount
    from source
)

select * from renamed
