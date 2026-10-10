select cd_category, lib_category 
from {{ref('_stg_category_deduplicated')}}
where rn = 1 
    and cd_category is not null
    and lib_category is not null