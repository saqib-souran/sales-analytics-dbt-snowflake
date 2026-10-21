-- Every accepted sales date must exist in the calendar.
select distinct
    s.dt_sale
from {{ ref('stg_sale') }} s
left join {{ ref('dim_calendar') }} c
    on s.dt_sale = c.dt_calendar
where c.id_dim_calendar is null