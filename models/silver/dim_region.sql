with distinct_category as
(
    select distinct 
        CODE_REGION,
        REGION_NAME 
    from {{ref('stg_file_zone_geographique')}}
),
deduplicated_region as
(
    select 
        CODE_REGION,
        REGION_NAME,
        row_number() over(partition by CODE_REGION order by REGION_NAME) as rn
    from distinct_category
)
select 
    {{ dbt_utils.generate_surrogate_key(['CODE_REGION']) }} as id_dim_region,
    CODE_REGION as CD_REGION,
    REGION_NAME,
    1 as id_dim_country
from deduplicated_region
where rn=1