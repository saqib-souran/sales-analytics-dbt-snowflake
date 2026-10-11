{% snapshot snap_customer %}
{{
    config(
        target_schema='silver',
        unique_key='customer_id',
        strategy='check',
        check_cols=['CUSTOMER_ID', 'LAST_NAME', 'FIRST_NAME', 'ADDRESS', 'POSTAL_CODE', 'CD_TYPE_CUSTOMER']
    )
}}
select 
    CUSTOMER_ID,
    LAST_NAME,
    FIRST_NAME,
    ADDRESS,
    POSTAL_CODE,
    CD_TYPE_CUSTOMER
from {{ref('stg_customer')}}
{% endsnapshot %}