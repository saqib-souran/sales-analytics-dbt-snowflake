select distinct
    row_number() over(order by CODE_COMMUNE_POSTAL) as id_dim_postal_code,
    CODE_COMMUNE_POSTAL as cd_postal,
    postal_commune_name,
    id_dim_department
from {{ref('stg_file_zone_geographique')}} zg
inner join {{ref('dim_department')}} d
on zg.code_department = d.cd_department