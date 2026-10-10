with cleaned_subcategory as 
(
select nullif(trim(cd_subcategory),'') as cd_subcategory, 
       nullif(trim(regexp_replace(initcap(lib_subcategory), '\\s+', ' ')),'') as lib_subcategory,
       nullif(trim(cd_category),'') as cd_category 
from {{source('bronze', 'sub_category')}}
)
select *, 
    row_number() over (
        partition by cd_subcategory 
        order by lib_subcategory
    ) as rn
from cleaned_subcategory