{{ config(
    materialized='table',
    tags=['business']
) }}

with supplier as (
    
    select
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_sk,
        supplier_id,
        name,
        nation_id,
        phone,
        account_balance
    from {{ ref('stg_supplier') }} 

)

select *
from supplier