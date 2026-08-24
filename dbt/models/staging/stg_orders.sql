with source as (
    select * from {{ source('public', 'orders') }} -- match with the schema name in your database pgAdmin4
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