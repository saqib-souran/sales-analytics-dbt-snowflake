with sale_detail as
(
    select 
        s.sale_id,
        s.dt_sale,
        s.customer_id,
        d.cd_product,
        d.quantity_sold,
        d.sale_price 
    from {{ref('stg_sale')}} s
        inner join {{ref('stg_detail_sale')}} d
            on s.sale_id = d.sale_id
), join_detail as
(
    select 
        sd.sale_id,
        ca.id_dim_calendar,
        c.id_dim_customer,
        p.id_dim_product,
        sd.quantity_sold,
        sd.sale_price 
    from sale_detail sd
        left outer join {{ref('dim_customer')}} c
            on sd.customer_id = c.customer_id
            --    and sd.dt_sale >= c.valid_from 
                and (sd.dt_sale < c.valid_to or c.valid_to is null)
        left outer join {{ref('dim_product')}} p
            on sd.cd_product = p.cd_product
            --    and sd.dt_sale >= p.valid_from 
                and (sd.dt_sale < p.valid_to or p.valid_to is null)
        left outer join {{ref('dim_calendar')}} ca
            on sd.dt_sale = ca.dt_calendar
        qualify row_number() over(
            partition by sd.sale_id, sd.cd_product
            order by c.valid_from desc, p.valid_from desc
        ) = 1
)
select 
    sale_id,
    id_dim_calendar,
    id_dim_customer,
    id_dim_product,
    quantity_sold,
    sale_price,
    quantity_sold * sale_price as revenue_amount,
    1 as sale_count
from join_detail
where id_dim_calendar is not null
and id_dim_customer is not null
and id_dim_product is not null