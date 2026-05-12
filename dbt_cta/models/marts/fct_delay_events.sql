with arrivals as (

    select * from {{ ref('stg_arrivals') }}

),

delay_events as (

    select
        _dlt_id                     as delay_event_id,
        station_id,
        stop_id,
        run_number,
        route,
        station_name,
        stop_description,
        destination_name,
        train_direction,
        prediction_generated_at,
        predicted_arrival_at,
        is_fault,
        is_scheduled,
        _dlt_load_id
    from arrivals
    where is_delayed = true

)

select * from delay_events
