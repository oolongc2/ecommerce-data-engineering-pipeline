with source as (
    select * from {{ source('ecommerce_raw', 'orders') }}
),

renamed as (
    select
        order_id,
        user_id,
        product_id,
        quantity,
        cast(total_price as numeric(10, 2)) as total_price,
        order_created_at as created_at
    from source
)

select * from renamed