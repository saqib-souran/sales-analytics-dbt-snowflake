with cleaned_category as 
(
select nullif(trim(cd_category),'') as cd_category, 
       nullif(trim(regexp_replace(initcap(lib_category), '\\s+', ' ')),'') as lib_category
from {{source('bronze', 'category')}}
)
select *, 
    row_number() over (
        partition by cd_category 
        order by lib_category
    ) as rn
from cleaned_category