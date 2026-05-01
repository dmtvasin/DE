with all_rides as (
    select *
    from {{ ref('stg_rides') }}
)

select *
from all_rides
where rn = 1 -- удаление дублей
and date_trunc('month', pickup_date) = '2077-10-01'