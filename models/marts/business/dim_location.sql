{{ config(
    tags=['business']
) }}

with location as (

    select
        {{ dbt_utils.generate_surrogate_key(['nation_id']) }} as nation_sk,
        nation_id, 
        nation_name,
        region_name
        from {{ ref('int_location') }} 
)

select * 
from location