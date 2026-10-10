{{
    config(
        materialized='incremental',
        unique_key='rejection_id'

    )
}}
select 
    {{dbt_utils.generate_surrogate_key(['cd_category', 'lib_category', 'rn'])}} as rejected_id,
    cd_category, lib_category,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    case
        when cd_category is null then 'missing category coed'
        when lib_category is null then 'missing category description'
        when rn>1 then 'deduplicate key'
    end as rejected_reason
from {{ref('_stg_category_deduplicated')}}
where rn > 1 
    or  cd_category is null
    or lib_category is null