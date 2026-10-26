with circuits as (
    select * from {{ ref('stg_f1_circuits') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['circuit_id']) }} as circuit_key,

    circuit_id,
    circuit_name,
    locality as circuit_locality,
    country as circuit_country,
    latitude as circuit_latitude,
    longitude as circuit_longitude

from circuits
