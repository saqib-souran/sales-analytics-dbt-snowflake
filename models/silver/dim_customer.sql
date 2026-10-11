select
    row_number() over (order by sc.dbt_scd_id) as id_dim_customer,
    sc.dbt_valid_from as valid_from,
    sc.dbt_valid_to as valid_to,
    case when sc.dbt_valid_to is null then 1 else 0 end as is_current_version,
    sc.CUSTOMER_ID,
    sc.LAST_NAME,
    sc.FIRST_NAME,
    sc.ADDRESS,
    pc.id_dim_postal_code,
    tc.id_dim_type_customer
from {{ref('snap_customer')}} sc
    left outer join {{ref('dim_postal_code')}} pc
        on sc.POSTAL_CODE = pc.cd_postal
    left outer join {{ref('dim_type_customer')}} tc
        on sc.CD_TYPE_CUSTOMER = tc.CD_TYPE_CUSTOMER
where pc.cd_postal is not null
and tc.CD_TYPE_CUSTOMER is not null