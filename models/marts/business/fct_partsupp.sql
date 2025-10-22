with partsupp as (

    select 
        {{ dbt_utils.generate_surrogate_key(['part_id', 'supplier_id']) }} as partsupp_sk,
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_sk,
        {{ dbt_utils.generate_surrogate_key(['part_id']) }} as part_sk,
        supply_cost,
        availability
    from {{ ref('stg_partsupp') }}

)

select *
from partsupp