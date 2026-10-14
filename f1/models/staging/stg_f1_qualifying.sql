with source as (

    select * from {{ source('raw_f1', 'qualifying') }}

),

renamed as (

    select
        sq.season                                                       as partition_season,
        sq.round                                                        as partition_round,
        sq._source_file                                                 as source_file,
        sq._loaded_at                                                   as loaded_at,

        concat_ws('-', r.value:season::string, r.value:round::string, q.value:Driver.driverId::string) as qualifying_id,

        concat_ws('-', r.value:season::string, r.value:round::string)   as race_id,
        q.value:Driver.driverId::string                                 as driver_id,
        q.value:Constructor.constructorId::string                       as constructor_id,

        r.value:season::int                                             as season,
        r.value:round::int                                              as round,
        q.value:number::int                                             as driver_number,
        q.value:position::int                                           as qualifying_position,

        nullif(q.value:Q1::string, '')                                  as q1_time,
        nullif(q.value:Q2::string, '')                                  as q2_time,
        nullif(q.value:Q3::string, '')                                  as q3_time

    from source sq,
        lateral flatten(input => sq.payload:RaceTable.Races) r,
        lateral flatten(input => r.value:QualifyingResults) q

)

select * from renamed