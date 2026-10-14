with source as (

    select * from {{ source('raw_f1', 'laps') }}

),

renamed as (

    select
        sl.season                                                       as partition_season,
        sl.round                                                        as partition_round,
        sl._source_file                                                 as source_file,
        sl._loaded_at                                                   as loaded_at,

        concat_ws('-', r.value:season::string, r.value:round::string, l.value:number::string, t.value:driverId::string) as lap_time_id,
        concat_ws('-', r.value:season::string, r.value:round::string)   as race_id,

        r.value:season::int                                             as season,
        r.value:round::int                                              as round,
        l.value:number::int                                             as lap_number,
        t.value:driverId::string                                        as driver_id,
        t.value:position::int                                           as position,
        t.value:time::string                                            as lap_time_formatted

    from source sl,
        lateral flatten(input => sl.payload:RaceTable.Races) r,
        lateral flatten(input => r.value:Laps) l,
        lateral flatten(input => l.value:Timings) t

)

select * from renamed