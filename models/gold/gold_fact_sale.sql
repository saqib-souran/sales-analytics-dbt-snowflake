{{ config(alias='fact_sale') }}
select
    sale_id,
    id_dim_calendar,
    id_dim_customer,
    id_dim_product,
    quantity_sold,
    sale_price,
    revenue_amount,
    sale_line_count
from {{ref('fact_sale')}}