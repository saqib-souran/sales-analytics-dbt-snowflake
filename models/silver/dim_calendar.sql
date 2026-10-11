WITH generate_date as
(
    SELECT 
        DATEADD(day, seq4(), '2015-01-01') AS DT_CALENDAR
    FROM TABLE(GENERATOR(ROWCOUNT => 4383))
)

select 
    TO_NUMBER(TO_CHAR(DT_CALENDAR, 'YYYYMMDD')) AS ID_DIM_CALENDAR,
    DT_CALENDAR, 
    DAY(DT_CALENDAR) AS NB_JOUR, 
    DECODE(DAYOFWEEK(DT_CALENDAR),
    0, 'Dimanche',
    1, 'Lundi',
    2, 'Mardi',
    3, 'Mercredi',
    4, 'Jeudi',
    5, 'Vendredi',
    6, 'Samedi'
    ) AS LB_JOUR, 
    MONTH(DT_CALENDAR) AS NB_MOIS, 

    DECODE(MONTH(DT_CALENDAR),
            1, 'Janvier',
            2, 'Février',
            3, 'Mars',
            4, 'Avril',
            5, 'Mai',
            6, 'Juin',
            7, 'Juillet',
            8, 'Août',
            9, 'Septembre',
            10, 'Octobre',
            11, 'Novembre',
            12, 'Décembre'
    ) AS  LB_MOIS, 

    DECODE(MONTH(DT_CALENDAR),
    1, 'Janv',
    2, 'Fév',
    3, 'Mars',
    4, 'Avril',
    5, 'Mai',
    6, 'Juin',
    7, 'Juil',
    8, 'Août',
    9, 'Sept',
    10, 'Oct',
    11, 'Nov',
    12, 'Déc'
        ) AS  LB_MOIS_COURT,                                                                                                                                                                                                                                                                                                                                                                                           
    QUARTER(DT_CALENDAR) AS NB_TRIMESTRE,
    'T' ||  QUARTER(DT_CALENDAR) AS LB_TRIMESTRE,
    
    CASE WHEN QUARTER(DT_CALENDAR) IN (1,2) THEN 1 ELSE 2 END AS NB_SEMESTRE,
    'S' ||  CASE WHEN QUARTER(DT_CALENDAR) IN (1,2) THEN 1 ELSE 2 END AS LB_SEMESTRE, 
    YEAR(DT_CALENDAR) AS NB_ANNEE
from generate_date