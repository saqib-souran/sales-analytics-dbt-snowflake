select
    sp.dbt_scd_id as id_dim_product,
    sp.dbt_valid_from as valid_from,
    sp.dbt_valid_to as valid_to,
    case when sp.dbt_valid_to is null then 1 else 0 end as is_current_version,
    sp.cd_product, 
    sp.lib_product, 
    sp.price_product, 
    scp.id_dim_subcategory
from {{ref('snap_product')}} sp
    left outer join {{ref('dim_subcategory')}} scp
        on sp.cd_subcategory = scp.cd_subcategory
where scp.cd_subcategory is not null