{{ config(materialized='view') }}

with trips as (
    select *
    from {{ ref('int_trips_unioned') }}
),

zones as (
    select *
    from {{ ref('dim_zones') }}
)

select
    -- surrogate key
    md5(
        cast(
            concat(
                vendor_id, '_',
                pickup_datetime, '_',
                taxi_type, '_',
                dropoff_datetime
            ) as varchar
        )
    ) as trip_id,

    -- identifiers
    t.vendor_id,
    t.rate_code_id,
    t.pickup_location_id,
    -- set default zone name here to avoid coalesce later
    coalesce(pz.zone, 'Unknown zone') as pickup_zone,
    pz.borough as pickup_borough,

    t.dropoff_location_id,
    coalesce(dz.zone, 'Unknown zone') as dropoff_zone,
    dz.borough as dropoff_borough,

    t.taxi_type as service_type,

    -- timestamps
    t.pickup_datetime,
    t.dropoff_datetime,

    -- duration (DuckDB syntax)
    date_diff('minute', t.pickup_datetime, t.dropoff_datetime) as trip_duration,

    -- trip info
    t.store_and_fwd_flag,
    t.passenger_count,
    t.trip_distance,
    t.trip_type,

    -- payment info
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.ehail_fee,
    t.improvement_surcharge,
    t.total_amount,
    t.payment_type

from trips t

left join zones pz
    on pz.location_id = t.pickup_location_id

left join zones dz
    on dz.location_id = t.dropoff_location_id