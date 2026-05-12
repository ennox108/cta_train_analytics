with source as (

    select * from {{ source('cta_raw', 'train_locations') }}

),

staged as (

    select
        -- keys
        run_number                                          as run_number,
        route                                               as route,

        -- descriptors
        destination_stop                                    as destination_stop,
        destination_name                                    as destination_name,
        train_direction                                     as train_direction,
        next_station_id                                     as next_station_id,
        next_stop_id                                        as next_stop_id,

        -- timestamp
        convert_timezone('UTC', timestamp::timestamp_tz)    as recorded_at,

        -- flags
        case when is_approaching = '1' then true else false end  as is_approaching,
        case when is_delayed     = '1' then true else false end  as is_delayed,

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
