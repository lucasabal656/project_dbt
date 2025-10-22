{{ config(
    materialized='table',
    tags=['business']
) }}

with dates as (

    select dateadd(day, seq4(), '1992-01-01') as full_date
    from table(generator(rowcount => 10000)) 

),

dim_date as (

    select
        row_number() over (order by full_date) as date_id,  -- surrogate key
        extract(year from full_date) as year,
        extract(month from full_date) as month,
        extract(day from full_date) as day,
        extract(quarter from full_date) as quarter
    from dates

)

select * from dim_date
