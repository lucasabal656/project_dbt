{{ 
    config(
        materialized='incremental',
        unique_key='orders_sk',
        incremental_strategy='merge',
        on_schema_change='sync',
        post_hook= "grant select on {{ this }} to role SYSADMIN"
    )
}}

with orders as (
    select *
    from {{ ref('int_orders') }}
    {% if is_incremental() %}
        -- Solo traemos nuevas órdenes o actualizadas
        where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

orders_enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
        {{ dbt_utils.generate_surrogate_key(['nation_id']) }} as nation_sk,
        {{ dbt_utils.generate_surrogate_key(['part_id']) }} as part_sk,
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_sk,
        {{ dbt_utils.generate_surrogate_key(['order_id', 'line_number', 'supplier_id']) }} as orders_sk,
        {{ calc_final_price('extended_price', 'discount', 'tax') }} as final_price,
        (supply_cost * quantity) as total_cost,
        {{ calc_final_price('extended_price', 'discount', 'tax') }} - (supply_cost * quantity) as profit,
        greatest(datediff(day, commit_date, ship_date), 0) as days_of_delay,
        datediff(day, ship_date, receipt_date) as shipping_days,
        order_id,
        line_number,
        customer_id,
        part_id,
        supplier_id,
        nation_id,
        extended_price,
        discount,
        tax,
        supply_cost,
        quantity,
        commit_date,
        ship_date,
        receipt_date,
        order_date
    from orders
)

select *
from orders_enriched
