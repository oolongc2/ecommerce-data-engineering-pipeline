with source as (
    select * from {{ source('public', 'users') }} -- match with the schema name in your database pgAdmin4
), -- attention to comma

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