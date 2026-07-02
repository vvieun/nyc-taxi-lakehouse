with trips as (
    select
        payment_type,
        total_amount,
        tip_amount
    from {{ source('gold', 'fact_trips') }}
),

payments as (
    select
        payment_type,
        payment_description
    from {{ source('gold', 'dim_payment_type') }}
)

select
    payments.payment_description,
    count(*) as trip_count,
    round(sum(trips.total_amount), 2) as total_revenue,
    round(avg(trips.tip_amount), 2) as avg_tip
from trips
inner join payments on trips.payment_type = payments.payment_type
group by payments.payment_description
order by trip_count desc
