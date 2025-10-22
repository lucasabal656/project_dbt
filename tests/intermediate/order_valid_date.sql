--Test para indicar que no hay envíos antes que su pedido

select *
from {{ ref('int_orders') }}
where ship_date < order_date
