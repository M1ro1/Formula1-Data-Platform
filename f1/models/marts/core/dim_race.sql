with races as (
    select * from {{ ref('stg_f1_races') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['race_id']) }} as race_key,

    race_id,
    season,
    round,
    race_name,
    race_date,
    race_time,
    to_number(to_char(race_date, 'YYYYMMDD'))               as race_date_key,

    {{ dbt_utils.generate_surrogate_key(['circuit_id']) }}  as circuit_key,

    first_practice_date,
    first_practice_time,
    second_practice_date,
    second_practice_time,
    third_practice_date,
    third_practice_time,
    qualifying_date,
    qualifying_time,
    sprint_date,
    sprint_time,
    sprint_shootout_date,
    sprint_shootout_time,
    sprint_qualifying_date,
    sprint_qualifying_time,
    has_sprint

from races