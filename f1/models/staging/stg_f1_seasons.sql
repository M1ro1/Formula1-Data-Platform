with source as (

    select * from {{ source('raw_f1', 'seasons') }}

),

renamed as (

    select
        _source_file                                                    as source_file,
        _loaded_at                                                      as loaded_at,

        s.value:season::int                                             as season,
        s.value:url::string                                             as season_url

    from source,
        lateral flatten(input => payload:SeasonTable.Seasons) s

)

select * from renamed