with drivers as (
    select * from {{ ref('stg_f1_drivers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['driver_id']) }} as driver_key,

    driver_id,
    driver_code,
    permanent_number    as driver_permanent_number,
    given_name          as driver_given_name,
    family_name         as driver_family_name,
    full_name           as driver_full_name,
    date_of_birth       as driver_date_of_birth,
    nationality         as driver_nationality

from drivers