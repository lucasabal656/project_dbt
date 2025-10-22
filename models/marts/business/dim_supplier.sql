with supplier as (
    
    select
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_sk,
        supplier_id,
        name,
        {{ dbt_utils.generate_surrogate_key(['nation_id']) }} as nation_sk,
        nation_id,
        phone,
        account_balance
    from {{ ref('stg_supplier') }} 

)

select *
from supplier