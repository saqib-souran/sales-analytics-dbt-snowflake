select cd_subcategory, lib_subcategory, cd_category
from {{ref('_stg_subcategory_deduplicated')}}
where rn= 1
    and cd_subcategory is not null
    and cd_category is not null
    and lib_subcategory is not null