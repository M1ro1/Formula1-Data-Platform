-- Grain: one row per constructor per season.
-- NOTE: points come from race results only (sprint points are not loaded).

with results as (
    select * from {{ ref('fct_race_results') }}
),

constructors as (
    select constructor_key, constructor_id, constructor_name
    from {{ ref('dim_constructor') }}
),

aggregated as (

    select
        season,
        constructor_key,

        count(distinct race_key)                                    as races,
        count(*)                                                    as car_entries,
        count_if(is_winner)                                         as wins,
        count_if(is_podium)                                         as podiums,
        count_if(has_fastest_lap)                                   as fastest_laps,
        sum(points)                                                 as race_points,
        count_if(is_classified)                                     as classified_finishes,
        round(count_if(is_classified) / count(*), 4)                as classified_rate,
        round(avg(case when is_classified then finish_position end), 2) as avg_finish_position

    from results
    group by season, constructor_key

)

select
    a.season,
    a.constructor_key,
    c.constructor_id,
    c.constructor_name,

    a.races,
    a.car_entries,
    a.wins,
    a.podiums,
    a.fastest_laps,
    a.race_points,
    a.classified_finishes,
    a.classified_rate,
    a.avg_finish_position,

    rank() over (
        partition by a.season order by a.race_points desc, a.wins desc
    )                                                               as rank_by_race_points

from aggregated a
left join constructors c on a.constructor_key = c.constructor_key