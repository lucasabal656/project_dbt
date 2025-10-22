{{ config(
    materialized='table',
    tags=['business']
) }}

with part as (

    select 
        {{ dbt_utils.generate_surrogate_key(['part_id']) }} as part_sk,
        part_id,
        name,
        brand,
        type,
        size,
        container,
        retail_price
    from {{ ref('stg_part') }}

),

clean_part as (

    select
        part_sk,
        part_id,
        name,
        regexp_replace(brand, '[^0-9]', '')::int as brand_id,
        initcap(split_part(type, ' ', 1)) as category, --División de type en 3 categorias para futuros análisis
        initcap(split_part(type, ' ', 2)) as finish,
        initcap(split_part(type, ' ', 3)) as material,
        size,
        initcap(container) as container,
        retail_price

    from part

)

select * from clean_part