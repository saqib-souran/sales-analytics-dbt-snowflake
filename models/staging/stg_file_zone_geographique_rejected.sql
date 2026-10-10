{{
    config(
        materialized='incremental',
        unique_key='rejection_id'
    )
}}
select 
        {{ dbt_utils.generate_surrogate_key(['code_commune_postal', 'postal_commune_name', 'code_postal', 'full_commune_name', 'code_department', 'department_name', 'code_region', 'region_name','rn']) }} as rejection_id,
        code_commune_postal, postal_commune_name, code_postal, full_commune_name, code_department, department_name, code_region, region_name,
        convert_timezone('Europe/Paris', current_timestamp()) as rejected_at, 
        case
                when code_commune_postal is null  then 'missing_commune_postal_code'
                when postal_commune_name is null  then 'missing_commune_postal_name'
                when code_postal is null          then 'missing_postal_code'
                when full_commune_name is null    then 'missing_commune_full_name'
                when code_department is null      then 'missing_department_code'
                when department_name is null      then 'missing_department_name'
                when code_region is null          then 'missing_region_code'
                when region_name is null          then 'missing_region_name'
                when rn > 1                       then 'duplicate_key_different_case'
        end as rejected_reason
from {{ ref('_stg_file_zone_geographique_deduplicated') }}
where 
    code_commune_postal is null
    or postal_commune_name is null
    or code_postal is null
    or full_commune_name is null
    or code_department is null
    or department_name is null
    or code_region is null
    or region_name is null
    or rn > 1