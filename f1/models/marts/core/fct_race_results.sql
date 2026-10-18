-- Grain: one row per driver per race.

with results as (
    select * from {{ ref('stg_f1_results') }}
),

races as (
    select race_id, race_date_key from {{ ref('dim_race') }}
),

prepared as (
    select
        res.result_id,
        res.race_id,
        res.driver_id,
        res.constructor_id,
        ra.race_date_key,

        res.season,
        res.round,

        res.car_number,
        res.grid_position,
        res.finish_position,
        res.position_text,
        res.status,
        res.points,
        res.completed_laps,

        try_to_number(res.position_text) is not null    as is_classified,

        case when res.status = 'Finished' then res.race_time_millis end as race_time_millis,

        res.fastest_lap_rank,
        res.fastest_lap_number,
        {{ lap_time_to_seconds('res.fastest_lap_time_formatted') }} as fastest_lap_time_seconds,
        res.fastest_lap_avg_speed_kph

    from results res
    left join races ra
        on res.race_id = ra.race_id
)

select
    {{ dbt_utils.generate_surrogate_key(['result_id']) }}           as result_key,
    result_id,

    {{ dbt_utils.generate_surrogate_key(['race_id']) }}             as race_key,
    {{ dbt_utils.generate_surrogate_key(['driver_id']) }}           as driver_key,
    {{ dbt_utils.generate_surrogate_key(['constructor_id']) }}      as constructor_key,
    race_date_key,

    season,
    round,

    car_number                                                      as driver_number,
    grid_position,
    finish_position,
    position_text,
    status,
    points,
    completed_laps,
    race_time_millis,

    fastest_lap_rank,
    fastest_lap_number,
    fastest_lap_time_seconds,
    fastest_lap_avg_speed_kph,

    is_classified,
    (is_classified and finish_position = 1)                         as is_winner,
    (is_classified and finish_position <= 3)                        as is_podium,
    (points > 0)                                                    as is_points_finish,
    (fastest_lap_rank = 1)                                          as has_fastest_lap,

    case
        when is_classified and grid_position > 0
            then grid_position - finish_position
    end                                                             as positions_gained

from prepared