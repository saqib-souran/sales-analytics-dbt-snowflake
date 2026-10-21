with prepared_detail_sale as (
    select
        nullif(trim(sale_id), '') as sale_id,

        nullif(trim(cd_product), '') as cd_product,

        nullif(
            trim(to_varchar(quantity_sold)),
            ''
        ) as quantity_text,

        nullif(
            replace(
                regexp_replace(
                    trim(to_varchar(sale_price)),
                    '[[:space:]]+',
                    ''
                ),
                ',',
                '.'
            ),
            ''
        ) as price_text

    from {{ source('bronze', 'detail_sale') }}
),

cleaned_detail_sale as (
    select
        sale_id,
        cd_product,

        -- Accept whole-number quantities without rounding fractions.
        case
            when regexp_like(quantity_text, '^[+-]?[0-9]+$')
                then try_to_decimal(quantity_text, 38, 0)
            else null
        end as quantity_sold,

        try_to_decimal(
            price_text,
            16,
            4
        ) as sale_price

    from prepared_detail_sale
)

select
    *,
    row_number() over (
        partition by sale_id, cd_product
        order by sale_id, cd_product
    ) as rn
from cleaned_detail_sale