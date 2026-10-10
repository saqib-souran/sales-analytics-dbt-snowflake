with cleaned_sale as 
(
select nullif(trim(SALE_ID),'') as SALE_ID,
coalesce(
            try_to_date(replace(trim(dt_sale), '/', '-'), 'YYYY-MM-DD'),
            try_to_date(replace(trim(dt_sale), '/', '-'), 'DD-MM-YYYY'),
            try_to_date(replace(trim(dt_sale), '/', '-'), 'MM-DD-YYYY')
       ) as dt_sale,
       nullif(trim(customer_id),'') as customer_id,
from {{source('bronze', 'sale')}}
)
select *, 
    row_number() over (
        partition by SALE_ID 
        order by dt_sale
    ) as rn
from cleaned_sale