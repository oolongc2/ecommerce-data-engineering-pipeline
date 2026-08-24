with source as (
    select * from {{ source('ecommerce_raw', 'users') }}
)

renamed as (
    select
        user_id,
        username,
        email,
        -- Standardize timestamp naming
        user_created_at as created_at
    from source
)

select * from renamed