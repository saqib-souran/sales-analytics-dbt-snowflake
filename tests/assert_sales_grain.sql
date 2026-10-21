with staging_duplicates as (
    select
        sale_id,
        cd_product,
        count(*) as row_count
    from {{ ref('stg_detail_sale') }}
    group by sale_id, cd_product
    having count(*) > 1
),

fact_duplicates as (
    select
        f.sale_id,
        p.cd_product,
        count(*) as row_count
    from {{ ref('fact_sale') }} f
    inner join {{ ref('dim_product') }} p
        on f.id_dim_product = p.id_dim_product
    group by f.sale_id, p.cd_product
    having count(*) > 1
)

select
    'stg_detail_sale' as model_name,
    sale_id,
    cd_product,
    row_count
from staging_duplicates

union all

select
    'fact_sale' as model_name,
    sale_id,
    cd_product,
    row_count
from fact_duplicates