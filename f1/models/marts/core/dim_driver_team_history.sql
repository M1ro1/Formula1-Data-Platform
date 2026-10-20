with appearances as (

    select
        r.driver_key,
        r.constructor_key,
        d.race_date,
        d.season,
        d.round
    from {{ ref('fct_race_results') }} r
    inner join {{ ref('dim_race') }} d
        on r.race_key = d.race_key

),

flagged as (

    select
        *,
        case
            when constructor_key = lag(constructor_key) over (
                partition by driver_key order by race_date, round
            ) then 0
            else 1
        end as is_new_stint
    from appearances

),

numbered as (

    select
        *,
        sum(is_new_stint) over (
            partition by driver_key
            order by race_date, round
            rows between unbounded preceding and current row
        ) as stint_number
    from flagged

),

stints as (

    select
        driver_key,
        constructor_key,
        stint_number,
        min(race_date)  as valid_from,
        max(race_date)  as last_race_date,
        min(season)     as first_season,
        max(season)     as last_season,
        count(*)        as races_in_stint
    from numbered
    group by driver_key, constructor_key, stint_number

),

with_validity as (

    select
        *,
        lead(valid_from) over (partition by driver_key order by valid_from) as valid_to
    from stints

)

select
    {{ dbt_utils.generate_surrogate_key(['driver_key', 'valid_from']) }}    as driver_team_history_key,
    driver_key,
    constructor_key,
    stint_number,
    valid_from,
    valid_to,
    last_race_date,
    first_season,
    last_season,
    races_in_stint,
    (valid_to is null)                                                      as is_latest_stint

from with_validity
