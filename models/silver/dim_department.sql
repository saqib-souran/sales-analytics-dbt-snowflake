with distinct_department as
(
    select distinct 
        CODE_DEPARTMENT,
        DEPARTMENT_NAME,
        CODE_REGION
    from {{ref('stg_file_zone_geographique')}}
),
deduplicated_department as
(
    select 
        CODE_DEPARTMENT,
        DEPARTMENT_NAME,
        CODE_REGION,
        row_number() over(partition by CODE_DEPARTMENT order by DEPARTMENT_NAME) as rn
    from distinct_department
)
select 
    row_number() over(order by CODE_DEPARTMENT) as id_dim_department,
    CODE_DEPARTMENT as CODE_DEPARTMENT,
    DEPARTMENT_NAME,
    id_dim_region
from deduplicated_department d
    inner join {{ref('dim_region')}} r 
    on d.code_region = r.cd_region
where rn=1