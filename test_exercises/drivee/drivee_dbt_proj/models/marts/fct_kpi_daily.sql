with rides as (
    select *
    from {{ ref('fct_rides') }}
    where is_valid_trip
)

select
    pickup_date,
    count(*) as rides,
    count(distinct pickup_location_id) as unique_from_locations,
    count(distinct dropoff_location_id) as unique_to_locations,
    sum(total_amount) as revenue_total,
    sum(fare_amount) as revenue_fare,
    sum(tip_amount) as tips,
    avg(fare_amount) as avg_fare,
    avg(total_amount) as avg_total,
    avg(trip_distance) as avg_distance,
    avg(trip_duration_min) as avg_duration_min,
    sum(case when payment_type_name = 'Бесплатно' then 1 else 0 end) * 1.0 / nullif(count(*), 0) as share_free,
    sum(case when payment_type_name = 'Не оплачено' then 1 else 0 end) * 1.0 / nullif(count(*), 0) as share_unpaid,
    sum(case when payment_type_name = 'Аннулировано' then 1 else 0 end) * 1.0 / nullif(count(*), 0) as share_cancelled
from rides
group by 1
