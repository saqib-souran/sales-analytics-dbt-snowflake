select 
    row_number() over(order by CD_CATEGORY) as id_dim_category,
    CD_CATEGORY, 
    LIB_CATEGORY 
from {{ref('stg_category')}}