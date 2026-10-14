with raw_races as (

    select * from {{ source('raw_f1', 'races') }}

),

renamed as (

    select
        rr.season                                                       as partition_season,
        rr.round                                                        as partition_round,
        rr._source_file                                                 as source_file,
        rr._loaded_at                                                   as loaded_at,

        concat_ws('-', r.value:season::string, r.value:round::string)   as race_id,

        r.value:season::int                                            as season,
        r.value:round::int                                             as round,
        r.value:raceName::string                                       as race_name,
        r.value:url::string                                            as race_url,
        r.value:date::date                                             as race_date,
        try_to_time(rtrim(r.value:time::string, 'Z'))                  as race_time,

        r.value:Circuit.circuitId::string                              as circuit_id,
        r.value:Circuit.circuitName::string                            as circuit_name,
        r.value:Circuit.url::string                                    as circuit_url,
        r.value:Circuit.Location.locality::string                      as circuit_locality,
        r.value:Circuit.Location.country::string                       as circuit_country,
        r.value:Circuit.Location.lat::float                            as circuit_latitude,
        r.value:Circuit.Location.long::float                           as circuit_longitude,

        r.value:FirstPractice.date::date                               as first_practice_date,
        try_to_time(rtrim(r.value:FirstPractice.time::string, 'Z'))    as first_practice_time,
        r.value:SecondPractice.date::date                              as second_practice_date,
        try_to_time(rtrim(r.value:SecondPractice.time::string, 'Z'))   as second_practice_time,
        r.value:ThirdPractice.date::date                               as third_practice_date,
        try_to_time(rtrim(r.value:ThirdPractice.time::string, 'Z'))    as third_practice_time,
        r.value:Qualifying.date::date                                  as qualifying_date,
        try_to_time(rtrim(r.value:Qualifying.time::string, 'Z'))       as qualifying_time,

        r.value:Sprint.date::date                                      as sprint_date,
        try_to_time(rtrim(r.value:Sprint.time::string, 'Z'))           as sprint_time,
        r.value:SprintShootout.date::date                              as sprint_shootout_date,
        try_to_time(rtrim(r.value:SprintShootout.time::string, 'Z'))   as sprint_shootout_time,
        r.value:SprintQualifying.date::date                            as sprint_qualifying_date,
        try_to_time(rtrim(r.value:SprintQualifying.time::string, 'Z')) as sprint_qualifying_time,

        (r.value:Sprint is not null)                                   as has_sprint

    from raw_races rr,
        lateral flatten(input => rr.payload:RaceTable.Races) r

)

select * from renamed