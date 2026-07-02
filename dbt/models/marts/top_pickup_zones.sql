with trips as (
    select
        pickup_location_id,
        total_amount
    from {{ source('gold', 'fact_trips') }}
),

zones as (
    select
        location_id,
        borough,
        zone
    from {{ source('gold', 'dim_zone') }}
)

select
    zones.borough,
    zones.zone,
    count(*) as trip_count,
    round(sum(trips.total_amount), 2) as total_revenue
from trips
inner join zones on trips.pickup_location_id = zones.location_id
group by zones.borough, zones.zone
order by trip_count desc
