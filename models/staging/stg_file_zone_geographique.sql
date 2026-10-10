select code_commune_postal, postal_commune_name, code_postal, full_commune_name, code_department, department_name, code_region, region_name
from {{ ref('_stg_file_zone_geographique_deduplicated') }}
where 
    code_commune_postal is not null
    and postal_commune_name is not null
    and code_postal is not null
    and full_commune_name is not null
    and code_department is not null
    and department_name is not null
    and code_region is not null
    and region_name is not null
    and rn = 1