select
    row_number() over(order by CD_SUBCATEGORY) as id_dim_subcategory,
    CD_SUBCATEGORY, 
    LIB_SUBCATEGORY, 
    id_dim_category 
from {{ref('stg_subcategory')}} sc
left outer join {{ref('dim_category')}} c
on sc.cd_category = c.cd_category
where c.cd_category is not null