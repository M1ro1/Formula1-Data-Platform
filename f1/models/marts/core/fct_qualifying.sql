-- Grain: one row per driver per race.

with qualifying as (
    select * from {{ ref('stg_f1_qualifying') }}
),

races as (
    select
        race_id,
        season,
        round,
        race_date_key
    from {{ ref('dim_race') }}
),

prepared as (
    select
        q.race_id,
        q.driver_id,
        q.constructor_id,
        ra.season,
        ra.round,
        ra.race_date_key,

        q.driver_number,
        q.qualifying_position,

        {{ lap_time_to_seconds('q.q1_time') }} as q1_seconds,
        {{ lap_time_to_seconds('q.q2_time') }} as q2_seconds,
        {{ lap_time_to_seconds('q.q3_time') }} as q3_seconds

    from qualifying as q
    left join races as ra
        on q.race_id = ra.race_id
)

select
    {{ dbt_utils.generate_surrogate_key(['race_id', 'driver_id']) }}
        as qualifying_key,

    {{ dbt_utils.generate_surrogate_key(['race_id']) }} as race_key,
    {{ dbt_utils.generate_surrogate_key(['driver_id']) }} as driver_key,
    {{ dbt_utils.generate_surrogate_key(['constructor_id']) }}
        as constructor_key,
    race_date_key,

    season,
    round,

    driver_number,
    qualifying_position,

    q1_seconds,
    q2_seconds,
    q3_seconds,

    nullif(
        least(
            coalesce(q1_seconds, 999999),
            coalesce(q2_seconds, 999999),
            coalesce(q3_seconds, 999999)
        ),
        999999
    ) as best_qualifying_seconds,

    (q2_seconds is not null) as reached_q2,
    (q3_seconds is not null) as reached_q3

from prepared
