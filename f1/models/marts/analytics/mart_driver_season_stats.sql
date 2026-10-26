-- Grain: one row per driver per season.
-- NOTE: points come from race results only. Sprint-race points are not loaded,
-- so race_points can differ from the official
-- championship totals in sprint seasons.

with results as (
    select * from {{ ref('fct_race_results') }}
),

poles as (
    select
        season,
        driver_key,
        count_if(qualifying_position = 1) as poles
    from {{ ref('fct_qualifying') }}
    group by season, driver_key
),

drivers as (
    select
        driver_key,
        driver_id,
        driver_code,
        driver_full_name
    from {{ ref('dim_driver') }}
),

aggregated as (

    select
        season,
        driver_key,

        count(*) as races_entered,
        count_if(is_winner) as wins,
        count_if(is_podium) as podiums,
        count_if(is_points_finish) as points_finishes,
        count_if(has_fastest_lap) as fastest_laps,
        count_if(not is_classified) as not_classified,
        sum(points) as race_points,

        round(avg(nullif(grid_position, 0)), 2) as avg_grid_position,
        round(avg(case when is_classified then finish_position end), 2)
            as avg_finish_position,
        min(case when is_classified then finish_position end)
            as best_finish_position,
        sum(positions_gained) as total_positions_gained

    from results
    group by season, driver_key

)

select
    a.season,
    a.driver_key,
    d.driver_id,
    d.driver_code,
    d.driver_full_name,

    a.races_entered,
    a.wins,
    a.podiums,
    a.points_finishes,
    a.fastest_laps,
    a.not_classified,
    a.race_points,
    a.avg_grid_position,
    a.avg_finish_position,
    a.best_finish_position,
    a.total_positions_gained,

    p.poles,

    rank() over (
        partition by a.season order by a.race_points desc, a.wins desc
    ) as rank_by_race_points

from aggregated as a
left join drivers as d on a.driver_key = d.driver_key
left join poles as p
    on
        a.season = p.season
        and a.driver_key = p.driver_key
