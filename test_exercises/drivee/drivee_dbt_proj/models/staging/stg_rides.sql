with source as (
    select *
    from {{ source('raw', 'rides') }}
),

typed as (
    select
        -- идентификаторы
        coalesce(dim_vendor.name, 'Не указан') as vendor_name,
        coalesce(dim_ratecode.name, 'Не указан') as ratecode_name,
        coalesce(dim_payment_type.name, 'Не указан') as payment_type_name,
        try_cast(flocationid as UINTEGER) as pickup_location_id,
        try_cast(tlocationid as UINTEGER) as dropoff_location_id,
        concat(try_cast(flocationid as UINTEGER), '->', try_cast(tlocationid as UINTEGER)) as route_id,
        -- datetime
        try_cast(pickup_datetime as timestamp) as pickup_datetime,
        try_cast(dropoff_datetime as timestamp) as dropoff_datetime,
        try_cast(passenger_count as DOUBLE) as passenger_count,
        Case When try_cast(passenger_count as DOUBLE) in (1) Then '1 (Единичные)'
            When try_cast(passenger_count as DOUBLE) in (2,3) Then '2-3 (Парные)'
            When try_cast(passenger_count as DOUBLE) IS NOT NULL Then '4+ (Групповые)'
            Else 'Неизвестно'
        End as passenger_count_group,
        -- числовые поля
        try_cast(trip_distance as DOUBLE) as trip_distance,
        try_cast(fare_amount as DOUBLE) as fare_amount,
        try_cast(extra as DOUBLE) as extra,
        try_cast(xcorp_tax as DOUBLE) as xcorp_tax,
        try_cast(tip_amount as DOUBLE) as tip_amount,
        try_cast(tolls_amount as DOUBLE) as tolls_amount,
        try_cast(improvement_surcharge as DOUBLE) as improvement_surcharge,
        try_cast(total_amount as DOUBLE) as total_amount,
        try_cast(extra as DOUBLE) + try_cast(xcorp_tax as DOUBLE) + try_cast(improvement_surcharge as DOUBLE) as surcharge_amount,
        str_and_fwd_flag
    from source as s
    left join {{ ref('dim_vendor') }} as dim_vendor on s.vendorid = dim_vendor.vendor_id
    left join {{ ref('dim_payment_type') }} as dim_payment_type on s.payment_type = dim_payment_type.payment_type_id
    left join {{ ref('dim_ratecode') }} as dim_ratecode on s.ratecodeid = dim_ratecode.ratecode_id
),

derived as (
    select
        *,

        -- суррогатный ключ
        md5(concat(
            vendor_name,
            pickup_datetime,
            dropoff_datetime,
            pickup_location_id,
            dropoff_location_id,
            total_amount
        )) as id,
        date_diff('minute', pickup_datetime, dropoff_datetime) as trip_duration_min,
        cast(pickup_datetime as DATE) as pickup_date,
        extract(hour from pickup_datetime) as pickup_hour,
        ((extract(dow from pickup_datetime) + 6) % 7) + 1 as pickup_dow,
        concat(
            try_cast(strftime(pickup_datetime, '%V') as varchar), 
            ' - ', 
            try_cast(strftime(pickup_datetime, '%Y') as varchar)
        ) as pickup_week,
        (pickup_hour in (7, 8, 9, 10, 16, 17, 18, 19)) as is_peak,
        (pickup_hour in (6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21)) as is_day,
        (pickup_hour in (22, 23, 0, 1, 2, 3, 4, 5)) as is_night,        
        (
            pickup_datetime is not null
            and dropoff_datetime is not null
            and dropoff_datetime >= pickup_datetime
            and coalesce(trip_distance, 0) >= 0
            and coalesce(fare_amount, 0) >= 0
            and coalesce(total_amount, 0) >= 0
        ) as is_valid_trip
    from typed
),

deduped as (
    select
    *,
    row_number() over (partition by id order by dropoff_datetime, passenger_count desc) as rn
    from derived
)

select
*
from deduped
