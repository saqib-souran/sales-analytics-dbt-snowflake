{{
    config(
        materialized='incremental',
        unique_key='rejection_id'

    )
}}
select 
    {{dbt_utils.generate_surrogate_key(['CD_TYPE_CUSTOMER', 'LIB_TYPE_CUSTOMER', 'rn'])}} as rejected_id, 
    CD_TYPE_CUSTOMER, LIB_TYPE_CUSTOMER,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    case
        when CD_TYPE_CUSTOMER is null then 'missing type_customer code'
        when LIB_TYPE_CUSTOMER is null then 'missing type_customer description'
        when rn>1 then 'deduplicate key'
    end as rejected_reason
from {{ref('_stg_type_customer_deduplicated')}}
where rn > 1 
    or  CD_TYPE_CUSTOMER is null
    or LIB_TYPE_CUSTOMER is null