with green_tripdata as (
    select *, 
        'green' as taxi_type
    from {{ ref('stg_green_tripdata') }}
), 

yellow_tripdata as (
    select *, 
        'yellow' as taxi_type
    from {{ ref('stg_yellow_tripdata') }}
), 

trips_unioned as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
)

select * from trips_unioned