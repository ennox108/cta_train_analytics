with arrivals as (

    select * from {{ ref('stg_arrivals') }}

)

select
    -- surrogate key
    _dlt_id                                         as arrival_id,

    -- keys
    station_id,
    stop_id,
    run_number,
    route,

    -- descriptors
    station_name,
    stop_description,
    destination_stop,
    destination_name,
    train_direction,

    -- timestamps
    prediction_generated_at,
    predicted_arrival_at,

    -- derived: minutes until arrival at time of prediction
    datediff(
        'minute',
        prediction_generated_at,
        predicted_arrival_at
    )                                               as minutes_until_arrival,

    -- flags
    is_approaching,
    is_scheduled,
    is_fault,
    is_delayed,

    -- position at time of prediction
    latitude,
    longitude,
    heading,

    -- load metadata
    _dlt_load_id

from arrivals
