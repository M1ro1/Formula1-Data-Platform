-- Grain: one row per pit stop.

with pit_stops as (
    select * from {{ ref('stg_f1_pit_stops') }}
),

races as (
    select race_id, season, round, race_date_key from {{ ref('dim_race') }}
),

drivers_teams as (
    select race_id, driver_id, constructor_id from {{ ref('stg_f1_results') }}
),

prepared as (
    select
        ps.race_id,
        ps.driver_id,
        dt.constructor_id,
        r.season,
        r.round,
        r.race_date_key,

        ps.stop_number,
        ps.lap_number,
        ps.duration_seconds

    from pit_stops ps
    left join races r
        on ps.race_id = r.race_id
    left join drivers_teams dt
        on ps.race_id = dt.race_id
        and ps.driver_id = dt.driver_id
)

select
    {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id', 'stop_number']) }} as pit_stop_key,

    {{ dbt_utils.generate_surrogate_key(['race_id']) }}             as race_key,
    {{ dbt_utils.generate_surrogate_key(['driver_id']) }}           as driver_key,
    {{ dbt_utils.generate_surrogate_key(['constructor_id']) }}      as constructor_key,
    race_date_key,

    season,
    round,

    stop_number,
    lap_number,
    duration_seconds

from prepared