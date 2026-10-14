with source as (

    select * from {{ source('raw_f1', 'constructors') }}

),

renamed as (

    select
        _source_file                                                    as source_file,
        _loaded_at                                                      as loaded_at,

        c.value:constructorId::string                                   as constructor_id,
        c.value:name::string                                            as constructor_name,
        c.value:nationality::string                                     as constructor_nationality,
        c.value:url::string                                             as constructor_url

    from source,
        lateral flatten(input => payload:ConstructorTable.Constructors) c

)

select * from renamed