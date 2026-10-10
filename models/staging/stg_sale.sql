select sale_id, dt_sale, customer_id
from {{ref('_stg_sale_deduplicated')}}
where rn= 1
    and sale_id is not null
    and dt_sale is not null
    and customer_id is not null