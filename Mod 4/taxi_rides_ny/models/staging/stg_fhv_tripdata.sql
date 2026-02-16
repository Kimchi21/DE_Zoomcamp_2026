select
    dispatching_base_num,
    Affiliated_base_number,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id,
    SR_Flag
from {{ source('raw_data', 'fhv_tripdata') }}
where dispatching_base_num is not null