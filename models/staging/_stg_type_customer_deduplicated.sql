with cleaned_type_customer as 
(
select nullif(trim(CD_TYPE_CUSTOMER),'') as CD_TYPE_CUSTOMER, 
       nullif(trim(regexp_replace(initcap(LIB_TYPE_CUSTOMER), '\\s+', ' ')),'') as LIB_TYPE_CUSTOMER
from {{source('bronze', 'type_customer')}}
)
select *, 
    row_number() over (
        partition by CD_TYPE_CUSTOMER 
        order by LIB_TYPE_CUSTOMER
    ) as rn
from cleaned_TYPE_CUSTOMER