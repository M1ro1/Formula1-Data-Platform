with source as (

    select * from {{ source('raw_f1', 'circuits') }}

),

renamed as (

    select
        _source_file as source_file,
        _loaded_at as loaded_at,

        c.value:circuitId::string as circuit_id,
        c.value:circuitName::string as circuit_name,
        c.value:url::string as circuit_url,

        c.value:Location.locality::string as locality,
        c.value:Location.country::string as country,
        c.value:Location.lat::float as latitude,
        c.value:Location.long::float as longitude

    from source,
        lateral flatten(input => payload:CircuitTable.Circuits) as c

)

select * from renamed
