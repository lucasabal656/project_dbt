--Test para comprobar que los clientes tienen un nombre válido

select *
from {{ ref('stg_customer') }}
where customer_name is null or trim(customer_name) = ''