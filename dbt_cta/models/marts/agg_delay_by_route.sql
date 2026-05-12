with arrivals as (

    select * from {{ ref('stg_arrivals') }}

),

by_route_hour as (

    select
        route,
        date_trunc('hour', prediction_generated_at)     as hour,
        count(*)                                        as total_predictions,
        count(case when is_delayed then 1 end)          as delayed_count,
        count(case when is_fault then 1 end)            as fault_count,
        count(case when is_scheduled then 1 end)        as scheduled_count,
        avg(
            datediff('minute', prediction_generated_at, predicted_arrival_at)
        )                                               as avg_minutes_until_arrival
    from arrivals
    group by 1, 2

)

select
    route,
    hour,
    total_predictions,
    delayed_count,
    fault_count,
    scheduled_count,
    avg_minutes_until_arrival,
    round(
        delayed_count / nullif(total_predictions, 0) * 100, 2
    )                                                   as delay_rate_pct
from by_route_hour
order by route, hour
