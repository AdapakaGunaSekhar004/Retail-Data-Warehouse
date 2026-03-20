with source as (
    select * from raw_sales
),
renamed as (
    select
        invoiceno                       as order_id,
        invoicedate::date               as order_date,
        invoicetime                     as order_time,
        stockcode                       as product_id,
        description                     as product_name,
        quantity::integer               as quantity,
        unitprice::numeric(10,2)        as unit_price,
        totalsale::numeric(10,2)        as total_amount,
        customerid                      as customer_id,
        country,
        isreturn::boolean               as is_return
    from source
    where isreturn = 'False'
)
select * from renamed
