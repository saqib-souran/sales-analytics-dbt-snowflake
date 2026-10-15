{{ config(alias='dim_customer') }}
with geographic_area as
(
    select 
        pc.id_dim_postal_code,
        pc.cd_postal,
        pc.postal_commune_name,
        
        d.cd_department,
        d.department_name,
        
        r.cd_region,
        r.region_name,
    
        c.cd_country,
        c.lib_country
    from {{ref('dim_postal_code')}} pc
        inner join {{ref('dim_department')}} d
            on pc.id_dim_department = d.id_dim_department
        inner join {{ref('dim_region')}} r
            on d.id_dim_region = r.id_dim_region
        inner join {{ref('dim_country')}} c
            on r.id_dim_country = c.id_dim_country
)        
select
    c.id_dim_customer,
    c.valid_from,
    c.valid_to,
    c.is_current_version,
    c.customer_id,
    c.last_name,
    c.first_name,
    c.address,
    c.id_dim_postal_code,
    tc.cd_type_customer,
    tc.lib_type_customer,
    ga.cd_postal,
    ga.postal_commune_name,
    ga.cd_department,
    ga.department_name,
    ga.cd_region,
    ga.region_name,
    ga.cd_country,
    ga.lib_country
from {{ref('dim_customer')}} c
    inner join {{ref('dim_type_customer')}} tc
        on c.id_dim_type_customer  = tc.ID_DIM_TYPE_CUSTOMER
    inner join geographic_area ga
        on c.id_dim_postal_code = ga.id_dim_postal_code