select CD_TYPE_CUSTOMER, LIB_TYPE_CUSTOMER
from {{ref('_stg_type_customer_deduplicated')}}
where rn= 1
    and CD_TYPE_CUSTOMER is not null
    and LIB_TYPE_CUSTOMER is not null