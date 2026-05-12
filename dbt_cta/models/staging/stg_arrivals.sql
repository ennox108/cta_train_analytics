with source as (

    select * from {{ source('cta_raw', 'arrivals') }}

),

staged as (

    select
        -- keys
        station_id                                          as station_id,
        stop_id                                             as stop_id,
        run_number                                          as run_number,

        -- descriptors
        station_name                                        as station_name,
        stop_description                                    as stop_description,
        route                                               as route,
        destination_stop                                    as destination_stop,
        destination_name                                    as destination_name,
        train_direction                                     as train_direction,

        -- timestamps
        try_to_timestamp(prediction_generated)              as prediction_generated_at,
        try_to_timestamp(predicted_arrival_time)            as predicted_arrival_at,

        -- flags (CTA API returns '0'/'1' strings)
        case when is_approaching = '1' then true else false end  as is_approaching,
        case when is_scheduled  = '1' then true else false end   as is_scheduled,
        case when is_fault      = '1' then true else false end   as is_fault,
        case when is_delayed    = '1' then true else false end   as is_delayed,

        -- position
        try_to_double(latitude)                             as latitude,
        try_to_double(longitude)                            as longitude,
        try_to_number(heading)                              as heading,

        -- dlt metadata
        _dlt_load_id,
        _dlt_id

    from source

)

select * from staged
