with raw_results as (
    select * from {{ source('raw_f1', 'results') }}
),

renamed as (
    select
        rr.season as partition_season,
        rr.round as partition_round,
        rr._source_file as source_file,
        rr._loaded_at as loaded_at,

        concat_ws(
            '-',
            r.value:season::string,
            r.value:round::string,
            res.value:Driver.driverId::string
        ) as result_id,
        concat_ws('-', r.value:season::string, r.value:round::string)
            as race_id,

        r.value:season::int as season,
        r.value:round::int as round,

        res.value:Driver.driverId::string as driver_id,
        res.value:Driver.permanentNumber::int as driver_permanent_number,
        res.value:Driver.code::string as driver_code,
        res.value:Driver.givenName::string as driver_given_name,
        res.value:Driver.familyName::string as driver_family_name,
        res.value:Driver.dateOfBirth::date as driver_date_of_birth,
        res.value:Driver.nationality::string as driver_nationality,

        res.value:Constructor.constructorId::string as constructor_id,
        res.value:Constructor.name::string as constructor_name,
        res.value:Constructor.nationality::string as constructor_nationality,

        -- Результати гонки
        res.value:number::int as car_number,
        res.value:grid::int as grid_position,
        res.value:position::int as finish_position,
        res.value:positionText::string as position_text,
        try_to_number(res.value:positionText::string) is not null
            as is_classified,
        res.value:points::number(5, 2) as points,
        res.value:laps::int as completed_laps,
        res.value:status::string as status,

        res.value:Time.millis::int as race_time_millis,
        res.value:Time.time::string as race_time_formatted,

        res.value:FastestLap.rank::int as fastest_lap_rank,
        res.value:FastestLap.lap::int as fastest_lap_number,
        res.value:FastestLap.Time.time::string as fastest_lap_time_formatted,
        res.value:FastestLap.AverageSpeed.speed::float
            as fastest_lap_avg_speed_kph

    from raw_results as rr,
        lateral flatten(input => rr.payload:RaceTable.Races) as r,
        lateral flatten(input => r.value:Results) as res
)

select * from renamed
