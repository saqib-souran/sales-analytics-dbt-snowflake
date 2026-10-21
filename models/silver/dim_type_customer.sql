select 
    {{ dbt_utils.generate_surrogate_key(['cd_type_customer']) }} as id_dim_type_customer, 
    cd_type_customer,
    lib_type_customer
from {{ref('stg_type_customer')}}