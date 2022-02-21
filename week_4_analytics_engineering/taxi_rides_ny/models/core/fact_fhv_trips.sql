{{ config(materialized='table') }}

with dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select
    tripid,
    dispatching_base_num,
    pickup_locationid,
    dropoff_locationid,
    pickup_datetime,
    dropoff_datetime,
    sr_flag
from {{ ref('stg_fhv_tripdata') }} as trips
inner join dim_zones as pickup_zone
on trips.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips.dropoff_locationid = dropoff_zone.locationid