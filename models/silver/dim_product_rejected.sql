select    
    sp.cd_product, 
    sp.lib_product, 
    sp.price_product, 
    sp.cd_subcategory,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    'orphan_subcategory' as rejected_reason
from {{ref('snap_product')}} sp
    left outer join {{ref('dim_subcategory')}} scp
        on sp.cd_subcategory = scp.cd_subcategory
where scp.cd_subcategory is null