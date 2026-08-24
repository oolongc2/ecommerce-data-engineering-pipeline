-- models/marts/monthly_customer_summary.sql

with customers as (
    -- Referencing your staging file!
    select * from {{ ref('stg_users') }}
),

orders as (
    -- Referencing your staging file!
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
    select
        customers.user_id,
        customers.username,
        count(orders.order_id) as total_orders_made,
        sum(orders.total_price) as lifetime_value,
        max(orders.created_at) as last_order_date
    from customers
    left join orders on customers.user_id = orders.user_id
    group by 1, 2
)

select * from customer_orders