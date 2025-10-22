--Test para comprobar que las ventas agregadas por producto no son negativas

select
    part_sk,
    sum(final_price) as total_sales
from {{ ref('fct_orders') }}
group by part_sk
having total_sales < 0

