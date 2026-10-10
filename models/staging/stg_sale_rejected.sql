{{
    config(
        materialized='incremental',
        unique_key='rejection_id'

    )
}}
select 
    {{
        dbt_utils.generate_surrogate_key(['sale_id', 'dt_sale', 'customer_id', 'rn'])
    }} as rejected_id, 
    sale_id, dt_sale, customer_id,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    case
        when sale_id is null then 'missing sale identifier'
        when dt_sale is null then 'missing date sale'
        when customer_id is null then 'missing customer identifier'
        when rn>1 then 'deduplicate key'
    end as rejected_reason
from {{ref('_stg_sale_deduplicated')}}
where rn > 1 
    or sale_id is null
    or dt_sale is null
    or customer_id is null