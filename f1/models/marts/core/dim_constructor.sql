with constructors as (
    select * from {{ ref('stg_f1_constructors') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['constructor_id']) }} as constructor_key,

    constructor_id,
    constructor_name,
    constructor_nationality,

from constructors