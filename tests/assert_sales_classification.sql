with expected as (
    select
        s.sale_id,
        d.cd_product,
        count(*) as expected_count
    from {{ ref('stg_sale') }} s
    inner join {{ ref('stg_detail_sale') }} d
        on s.sale_id = d.sale_id
    group by s.sale_id, d.cd_product
),

accepted as (
    select
        f.sale_id,
        p.cd_product,
        count(*) as accepted_count
    from {{ ref('fact_sale') }} f
    inner join {{ ref('dim_product') }} p
        on f.id_dim_product = p.id_dim_product
    group by f.sale_id, p.cd_product
),

rejected as (
    select
        sale_id,
        cd_product,
        count(*) as rejected_count
    from {{ ref('fact_sale_rejected') }}
    group by sale_id, cd_product
),

all_keys as (
    select sale_id, cd_product from expected
    union
    select sale_id, cd_product from accepted
    union
    select sale_id, cd_product from rejected
),

comparison as (
    select
        k.sale_id,
        k.cd_product,
        coalesce(e.expected_count, 0) as expected_count,
        coalesce(a.accepted_count, 0) as accepted_count,
        coalesce(r.rejected_count, 0) as rejected_count
    from all_keys k
    left join expected e
        on k.sale_id = e.sale_id
        and k.cd_product = e.cd_product
    left join accepted a
        on k.sale_id = a.sale_id
        and k.cd_product = a.cd_product
    left join rejected r
        on k.sale_id = r.sale_id
        and k.cd_product = r.cd_product
)

select *
from comparison
where expected_count <> accepted_count + rejected_count
    or accepted_count > 1
    or rejected_count > 1
    or (
        accepted_count > 0
        and rejected_count > 0
    )