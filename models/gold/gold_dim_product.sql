{{ config(alias='dim_product') }}
select
    p.id_dim_product,
    p.valid_from,
    p.valid_to,
    p.is_current_version,
    p.cd_product,
    p.lib_product,
    p.price_product,
    sc.cd_subcategory,
    sc.lib_subcategory,
    c.cd_category,
    c.lib_category    
from {{ref('dim_product')}} p 
    inner join {{ref('dim_subcategory')}} sc
        on p.id_dim_subcategory = sc.id_dim_subcategory
    inner join {{ref('dim_category')}} c
        on sc.id_dim_category = c.id_dim_category