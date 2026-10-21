with date_bounds as (
    select
        date_trunc('year', min(dt_sale))::date as start_date,
        last_day(max(dt_sale), 'year')::date as end_date
    from {{ ref('stg_sale') }}
    where dt_sale is not null
),

generated_dates as (
    select
        dateadd(
            day,
            offsets.value::integer,
            bounds.start_date
        )::date as dt_calendar
    from date_bounds bounds,
        lateral flatten(
            input => array_generate_range(
                0,
                datediff(
                    day,
                    bounds.start_date,
                    bounds.end_date
                ) + 1
            )
        ) offsets
)

select
    to_number(
        to_char(dt_calendar, 'YYYYMMDD')
    ) as id_dim_calendar,

    dt_calendar,

    day(dt_calendar) as nb_jour,

    case dayofweekiso(dt_calendar)
        when 1 then 'Lundi'
        when 2 then 'Mardi'
        when 3 then 'Mercredi'
        when 4 then 'Jeudi'
        when 5 then 'Vendredi'
        when 6 then 'Samedi'
        when 7 then 'Dimanche'
    end as lb_jour,

    month(dt_calendar) as nb_mois,

    case month(dt_calendar)
        when 1 then 'Janvier'
        when 2 then 'Février'
        when 3 then 'Mars'
        when 4 then 'Avril'
        when 5 then 'Mai'
        when 6 then 'Juin'
        when 7 then 'Juillet'
        when 8 then 'Août'
        when 9 then 'Septembre'
        when 10 then 'Octobre'
        when 11 then 'Novembre'
        when 12 then 'Décembre'
    end as lb_mois,

    case month(dt_calendar)
        when 1 then 'Janv'
        when 2 then 'Fév'
        when 3 then 'Mars'
        when 4 then 'Avr'
        when 5 then 'Mai'
        when 6 then 'Juin'
        when 7 then 'Juil'
        when 8 then 'Août'
        when 9 then 'Sept'
        when 10 then 'Oct'
        when 11 then 'Nov'
        when 12 then 'Déc'
    end as lb_mois_court,

    quarter(dt_calendar) as nb_trimestre,

    'T' || quarter(dt_calendar) as lb_trimestre,

    case
        when month(dt_calendar) <= 6 then 1
        else 2
    end as nb_semestre,

    case
        when month(dt_calendar) <= 6 then 'S1'
        else 'S2'
    end as lb_semestre,

    year(dt_calendar) as nb_annee

from generated_dates