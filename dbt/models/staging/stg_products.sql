with source as (
    select * from {{ source('ecommerce_raw', 'products') }}
),

renamed as (
    select 
        product_id,
        product_name,
        cast(price as numeric(10, 2)) as price,
        product_created_at as created_at
    from source
)

select * from renamed