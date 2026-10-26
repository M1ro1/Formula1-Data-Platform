-- Grain: one row per driver per lap per race.
-- Incremental: only rows loaded since the last run are processed; the MERGE on
-- lap_time_key makes a re-loaded partition replace
-- its old rows instead of duplicating them.

{{ config(

    materialized='incremental',
    unique_key='lap_time_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

with laps as (
    select * from {{ ref('stg_f1_laps') }}

    {% if is_incremental() %}
        where
            loaded_at
            > (
                select coalesce(max(loaded_at), '1900-01-01'::timestamp_ntz)
                from {{ this }}
            )
    {% endif %}
),

races as (
    select
        race_id,
        season,
        round,
        race_date_key
    from {{ ref('dim_race') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['l.race_id', 'l.driver_id', 'l.lap_number']) }} as lap_time_key, --noqa

    {{ dbt_utils.generate_surrogate_key(['l.race_id']) }} as race_key,
    {{ dbt_utils.generate_surrogate_key(['l.driver_id']) }} as driver_key,
    ra.race_date_key,

    ra.season,
    ra.round,

    l.lap_number,
    l.position,

    {{ lap_time_to_seconds('l.lap_time_formatted') }} as lap_time_seconds,

    l.loaded_at

from laps as l
left join races as ra
    on l.race_id = ra.race_id
