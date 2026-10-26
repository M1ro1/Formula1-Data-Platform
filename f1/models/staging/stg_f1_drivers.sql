with source as (

    select * from {{ source('raw_f1', 'drivers') }}

),

renamed as (

    select
        _source_file as source_file,
        _loaded_at as loaded_at,

        d.value:driverId::string as driver_id,
        d.value:code::string as driver_code,
        d.value:permanentNumber::int as permanent_number,

        d.value:givenName::string as given_name,
        d.value:familyName::string as family_name,
        d.value:dateOfBirth::date as date_of_birth,
        d.value:nationality::string as nationality,
        d.value:url::string as driver_url,
        concat_ws(' ', d.value:givenName::string, d.value:familyName::string)
            as full_name

    from source,
        lateral flatten(input => payload:DriverTable.Drivers) as d

)

select * from renamed
