with source as (

    select * from {{ source('raw_f1', 'pit_stops') }}

),

renamed as (

    select
        sp.season                                                       as partition_season,
        sp.round                                                        as partition_round,
        sp._source_file                                                 as source_file,
        sp._loaded_at                                                   as loaded_at,

        concat_ws('-', r.value:season::string, r.value:round::string, p.value:driverId::string, p.value:stop::string) as pit_stop_id,

        concat_ws('-', r.value:season::string, r.value:round::string)   as race_id,

        r.value:season::int                                             as season,
        r.value:round::int                                              as round,
        p.value:driverId::string                                        as driver_id,
        p.value:stop::int                                               as stop_number,
        p.value:lap::int                                                as lap_number,
        try_to_time(p.value:time::string)                               as pit_time,
        p.value:duration::float                                         as duration_seconds

    from source sp,
        lateral flatten(input => sp.payload:RaceTable.Races) r,
        lateral flatten(input => r.value:PitStops) p

)

select * from renamed