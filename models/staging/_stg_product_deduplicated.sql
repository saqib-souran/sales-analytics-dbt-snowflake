with cleaned_product as (
    select
        nullif(trim(cd_product), '') as cd_product,

        nullif(
            trim(
                regexp_replace(
                    initcap(lib_product),
                    '[[:space:]]+',
                    ' '
                )
            ),
            ''
        ) as lib_product,

        try_to_decimal(
            nullif(
                replace(
                    regexp_replace(
                        trim(to_varchar(price_product)),
                        '[[:space:]]+',
                        ''
                    ),
                    ',',
                    '.'
                ),
                ''
            ),
            16,
            4
        ) as price_product,

        nullif(trim(cd_subcategory), '') as cd_subcategory

    from {{ source('bronze', 'product') }}
)

select
    *,
    row_number() over (
        partition by cd_product
        order by lib_product
    ) as rn
from cleaned_product