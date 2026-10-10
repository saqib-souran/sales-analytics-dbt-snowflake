{{
    config(
        materialized='incremental',
        unique_key='rejection_id'

    )
}}
select 
    {{dbt_utils.generate_surrogate_key(['cd_subcategory', 'lib_subcategory', 'cd_category', 'rn'])}} as rejected_id,
    cd_subcategory, lib_subcategory, cd_category,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    case
        when cd_subcategory is null then 'missing subcategory coed'
        when lib_subcategory is null then 'missing category description'
        when cd_category is null then 'missing category coed'
        when rn>1 then 'deduplicate key'
    end as rejected_reason
from {{ref('_stg_subcategory_deduplicated')}}
where rn > 1 
    or  cd_subcategory is null
    or lib_subcategory is null
    or cd_category is null