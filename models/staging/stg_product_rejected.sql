{{
    config(
        materialized='incremental',
        unique_key='rejection_id'

    )
}}
select 
    {{
        dbt_utils.generate_surrogate_key(['CD_PRODUCT', 'LIB_PRODUCT', 'PRICE_PRODUCT', 'CD_SUBCATEGORY', 'rn'])
    }} as rejected_id, 
    CD_PRODUCT, LIB_PRODUCT, PRICE_PRODUCT, CD_SUBCATEGORY,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    case
        when CD_PRODUCT is null then 'missing category code'
        when LIB_PRODUCT is null then 'missing category description'
        when PRICE_PRODUCT is null then 'missing price product'
        when CD_SUBCATEGORY is null then 'missing code sub category'
        when rn>1 then 'deduplicate key'
    end as rejected_reason
from {{ref('_stg_product_deduplicated')}}
where rn > 1 
    or  CD_PRODUCT is null
    or LIB_PRODUCT is null
    or  PRICE_PRODUCT is null
    or  CD_SUBCATEGORY is null