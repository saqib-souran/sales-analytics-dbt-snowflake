select
    f.sale_id,
    f.id_dim_customer,
    f.id_dim_product,
    f.id_dim_calendar
from {{ ref('fact_sale') }} f

left join {{ ref('dim_customer') }} c
    on f.id_dim_customer = c.id_dim_customer

left join {{ ref('dim_product') }} p
    on f.id_dim_product = p.id_dim_product

left join {{ ref('dim_calendar') }} d
    on f.id_dim_calendar = d.id_dim_calendar

where c.id_dim_customer is null
    or p.id_dim_product is null
    or d.id_dim_calendar is null