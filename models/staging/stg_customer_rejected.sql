{{
    config(
        materialized='incremental',
        unique_key='rejection_id'

    )
}}
select 
    {{
        dbt_utils.generate_surrogate_key(['CUSTOMER_ID', 'LAST_NAME', 'FIRST_NAME', 'ADDRESS', 'POSTAL_CODE', 'CITY', 'COUNTRY', 'CD_TYPE_CUSTOMER', 'rn'])
    }} as rejection_id, 
    CUSTOMER_ID, LAST_NAME, FIRST_NAME, ADDRESS, POSTAL_CODE, CITY, COUNTRY, CD_TYPE_CUSTOMER,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    case
        when CUSTOMER_ID is null then 'missing customer identifier'
        when LAST_NAME is null then 'missing customer last name'
        when POSTAL_CODE is null then 'missing customer postal code'
        when CD_TYPE_CUSTOMER is null then 'missing customer type'
        when rn>1 then 'deduplicate key'
    end as rejected_reason
from {{ref('_stg_customer_deduplicated')}}
where rn > 1 
    or CUSTOMER_ID is null
    or LAST_NAME is null
    or POSTAL_CODE is null
    or CD_TYPE_CUSTOMER is null