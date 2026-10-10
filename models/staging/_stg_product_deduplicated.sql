with cleaned_PRODUCT as 
(
select nullif(trim(CD_PRODUCT),'') as CD_PRODUCT, 
       nullif(trim(regexp_replace(initcap(LIB_PRODUCT), '\\s+', ' ')),'') as LIB_PRODUCT,
       nullif(replace(trim(regexp_replace(initcap(PRICE_PRODUCT), '\\s+', ' ')),',', '.'), '')::numeric(16, 4) as PRICE_PRODUCT,
       nullif(trim(CD_SUBCATEGORY),'') as CD_SUBCATEGORY
from {{source('bronze','product')}}
)
select *, 
    row_number() over (
        partition by CD_PRODUCT 
        order by LIB_PRODUCT
    ) as rn
from cleaned_PRODUCT