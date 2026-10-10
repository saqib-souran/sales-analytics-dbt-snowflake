select SALE_ID, CD_PRODUCT, QUANTITY_SOLD, SALE_PRICE
from {{ref('_stg_detail_sale_deduplicated')}}
where rn= 1
    and SALE_ID is not null
    and CD_PRODUCT is not null
    and QUANTITY_SOLD is not null
    and SALE_PRICE is not null