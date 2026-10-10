with cleaned_file_zone_geographique as
(
    select  
        nullif(LPAD(trim(code_commune_postal),5,'0'),'') as code_commune_postal, 
        nullif(trim(regexp_replace(initcap(nom_commune_postal), '\\s+', ' ')),'') as postal_commune_name, 
        nullif(lpad(trim(code_postal),5,'0'),'') as code_postal,
        nullif(trim(regexp_replace(initcap(nom_commune_complet), '\\s+', ' ')),'') as full_commune_name, 
        case 
                when nullif(trim(code_departement),'') is null then null
                when length(trim(code_departement)) = 1 THEN LPAD(trim(code_departement),2, '0') 
                else trim(code_departement)
        end as code_department,
        nullif(trim(regexp_replace(initcap(nom_departement), '\\s+', ' ')),'') as department_name,
        case 
                when nullif(trim(code_region),'') is null then null
                when length(trim(code_region)) = 1 THEN LPAD(trim(code_region),2, '0') 
                else trim(code_region)
        end as code_region,
        
        nullif(trim(regexp_replace(initcap(nom_region), '\\s+', ' ')),'') as region_name

    from {{ ref('file_zone_geographique') }}
)
select *, 
    row_number() over (
        partition by code_commune_postal 
        order by postal_commune_name
    ) as rn
from cleaned_file_zone_geographique