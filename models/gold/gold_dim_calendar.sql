--calendar
{{ config(alias='dim_calendar') }}
with select_min_max_date as
(
    select  
        min(id_dim_calendar) as min_dt, 
        max(id_dim_calendar) as max_dt
    from {{ref('fact_sale')}}
)
select
    c.id_dim_calendar,
    c.dt_calendar,
    c.nb_jour,
    c.lb_jour,
    c.nb_mois,
    c.lb_mois,
    c.lb_mois_court,
    c.nb_trimestre,
    c.lb_trimestre,
    c.nb_semestre,
    c.lb_semestre,
    c.nb_annee
from {{ref('dim_calendar')}} c
    inner join select_min_max_date mmd
        on c.id_dim_calendar between min_dt and max_dt