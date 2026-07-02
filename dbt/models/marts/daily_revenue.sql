with trips as (
    select
        pickup_date,
        total_amount,
        tip_amount,
        trip_distance
    from {{ source('gold', 'fact_trips') }}
)

select
    pickup_date,
    count(*) as trip_count,
    round(sum(total_amount), 2) as total_revenue,
    round(sum(tip_amount), 2) as total_tips,
    round(avg(total_amount), 2) as avg_fare,
    round(avg(trip_distance), 2) as avg_distance
from trips
group by pickup_date
order by pickup_date
