{% snapshot snap_constructors %}
{{config(

    schema='snapshots',
    unique_key='constructor_id',
    strategy='check',
    check_cols=['constructor_name', 'constructor_nationality']
)   }}

select
    constructor_id,
    constructor_name,
    constructor_nationality
from {{ ref('stg_f1_constructors') }}

{% endsnapshot %}