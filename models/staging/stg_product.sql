select CD_PRODUCT, LIB_PRODUCT, PRICE_PRODUCT, CD_SUBCATEGORY
from {{ref('_stg_product_deduplicated')}}
where rn= 1
    and CD_PRODUCT is not null
    and LIB_PRODUCT is not null
    and PRICE_PRODUCT is not null
    and CD_SUBCATEGORY is not null