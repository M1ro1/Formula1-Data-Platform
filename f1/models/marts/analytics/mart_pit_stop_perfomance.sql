-- Grain: one row per constructor per season.
-- Pit stops under red flags or with repairs can last minutes. Stops longer than
-- `pit_stop_outlier_seconds` (default 60, override with --vars) are counted separately
-- and excluded from the "clean" average.

{% set outlier_seconds = var('pit_stop_outlier_seconds', 60) %}

with pit_stops as (

    select * from {{ ref('fct_pit_stops') }}
    where constructor_key is not null
      and duration_seconds is not null

),

constructors as (

    select constructor_key, constructor_id, constructor_name
    from {{ ref('dim_constructor') }}

),

aggregated as (

    select
        season,
        constructor_key,

        count(*)                                                        as pit_stops,
        count(distinct race_key)                                        as races_with_stops,
        count_if(duration_seconds > {{ outlier_seconds }})              as outlier_stops,

        round(avg(case when duration_seconds <= {{ outlier_seconds }}
                       then duration_seconds end), 3)                   as avg_clean_duration_seconds,
        round(median(duration_seconds), 3)                              as median_duration_seconds,
        min(duration_seconds)                                           as fastest_stop_seconds

    from pit_stops
    group by season, constructor_key

)

select
    a.season,
    a.constructor_key,
    c.constructor_id,
    c.constructor_name,

    a.pit_stops,
    a.races_with_stops,
    a.outlier_stops,
    a.avg_clean_duration_seconds,
    a.median_duration_seconds,
    a.fastest_stop_seconds,

    rank() over (
        partition by a.season order by a.avg_clean_duration_seconds asc nulls last
    )                                                                   as rank_by_avg_clean_duration

from aggregated a
left join constructors c on a.constructor_key = c.constructor_key