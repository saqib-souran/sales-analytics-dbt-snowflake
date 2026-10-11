{{
    config(
        materialized='incremental',
        unique_key='rejection_id'

    )
}}
select 
    {{
        dbt_utils.generate_surrogate_key(['SALE_ID', 'CD_PRODUCT', 'QUANTITY_SOLD', 'SALE_PRICE', 'rn'])
    }} as rejection_id, 
    SALE_ID, CD_PRODUCT, QUANTITY_SOLD, SALE_PRICE,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    case
        when SALE_ID is null then 'missing sale identifier'
        when CD_PRODUCT is null then 'missing code product'
        when QUANTITY_SOLD is null then 'missing quantity sold'
        when SALE_PRICE is null then 'missing sale price'
        when rn>1 then 'deduplicate key'
    end as rejected_reason
from {{ref('_stg_detail_sale_deduplicated')}}
where rn > 1 
    or SALE_ID is null
    or CD_PRODUCT is null
    or QUANTITY_SOLD is null
    or SALE_PRICE is null