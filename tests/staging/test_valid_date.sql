-- Test para comprobar que una fecha está dentro de un rango de valores establecido

with invalid_dates as (

    select
        order_id,
        order_date
    from {{ ref('stg_orders') }}
    where
        order_date > current_date
        or order_date < '1992-01-01'
)

select * from invalid_dates