select
    sc.CUSTOMER_ID,
    sc.LAST_NAME,
    sc.FIRST_NAME,
    sc.ADDRESS,
    sc.POSTAL_CODE,
    sc.CD_TYPE_CUSTOMER,
    convert_timezone('Europe/Paris', current_timestamp()) as rejected_at,
    case
        when pc.cd_postal is null then 'missing postal code'
        when tc.CD_TYPE_CUSTOMER is null then 'missing type_customer'
    end as rejected_reason
from {{ref('snap_customer')}} sc
    left outer join {{ref('dim_postal_code')}} pc
        on sc.POSTAL_CODE = pc.cd_postal
    left outer join {{ref('dim_type_customer')}} tc
        on sc.CD_TYPE_CUSTOMER = tc.CD_TYPE_CUSTOMER
where pc.cd_postal is null
or tc.CD_TYPE_CUSTOMER is null