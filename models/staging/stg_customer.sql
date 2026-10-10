select CUSTOMER_ID, LAST_NAME, FIRST_NAME, ADDRESS, POSTAL_CODE, CITY, COUNTRY, CD_TYPE_CUSTOMER
from {{ref('_stg_customer_deduplicated')}}
where rn= 1
    and CUSTOMER_ID is not null
    and LAST_NAME is not null
    and POSTAL_CODE is not null
    and CD_TYPE_CUSTOMER is not null