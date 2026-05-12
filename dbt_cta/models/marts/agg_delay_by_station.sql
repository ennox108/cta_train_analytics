with arrivals as (

    select * from {{ ref('stg_arrivals') }}

),

by_station as (

    select
        station_id,
        station_name,
        route,
        count(*)                                        as total_predictions,
        count(case when is_delayed then 1 end)          as delayed_count,
        count(case when is_fault then 1 end)            as fault_count,
        avg(
            datediff('minute', prediction_generated_at, predicted_arrival_at)
        )                                               as avg_minutes_until_arrival,
        min(prediction_generated_at)                    as first_seen_at,
        max(prediction_generated_at)                    as last_seen_at
    from arrivals
    group by 1, 2, 3

)

select
    station_id,
    station_name,
    route,
    total_predictions,
    delayed_count,
    fault_count,
    avg_minutes_until_arrival,
    first_seen_at,
    last_seen_at,
    round(
        delayed_count / nullif(total_predictions, 0) * 100, 2
    )                                                   as delay_rate_pct
from by_station
order by delay_rate_pct desc nulls last
