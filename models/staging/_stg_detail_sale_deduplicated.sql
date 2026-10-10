with cleaned_DETAIL_SALE as 
(
    select 
        nullif(trim(SALE_ID),'') as SALE_ID, 
        nullif(trim(CD_PRODUCT),'') as CD_PRODUCT,
        nullif(trim(QUANTITY_SOLD),'')::INTEGER as QUANTITY_SOLD,
        nullif(replace(trim(regexp_replace(SALE_PRICE, '\\s+', ' ')),',', '.'), '')::numeric(16, 4)       as SALE_PRICE
    from {{source('bronze', 'detail_sale')}}
)
select *, 
    row_number() over (
        partition by SALE_ID, CD_PRODUCT
        order by SALE_ID, CD_PRODUCT
    ) as rn
from cleaned_DETAIL_SALE