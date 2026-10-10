with cleaned_customer as 
(
select nullif(trim(CUSTOMER_ID),'') as CUSTOMER_ID, 
       nullif(trim(regexp_replace(initcap(LAST_NAME), '\\s+', ' ')),'') as LAST_NAME,
       nullif(trim(regexp_replace(initcap(FIRST_NAME), '\\s+', ' ')),'') as FIRST_NAME,
       nullif(trim(regexp_replace(initcap(ADDRESS), '\\s+', ' ')),'') as ADDRESS,
       nullif(trim(regexp_replace(POSTAL_CODE, '\\s+', ' ')),'') as POSTAL_CODE,
       nullif(trim(regexp_replace(CITY, '\\s+', ' ')),'') as CITY,
       nullif(trim(regexp_replace(initcap(COUNTRY), '\\s+', ' ')),'') as COUNTRY,
       nullif(trim(CD_TYPE_CUSTOMER),'') as CD_TYPE_CUSTOMER
from {{source('bronze','customer')}}
)
select *, 
    row_number() over (
        partition by CUSTOMER_ID 
        order by LAST_NAME, FIRST_NAME
    ) as rn
from cleaned_customer