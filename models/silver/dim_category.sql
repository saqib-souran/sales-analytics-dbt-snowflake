select 
    {{ dbt_utils.generate_surrogate_key(['CD_CATEGORY']) }} as id_dim_category,
    CD_CATEGORY, 
    LIB_CATEGORY 
from {{ref('stg_category')}}