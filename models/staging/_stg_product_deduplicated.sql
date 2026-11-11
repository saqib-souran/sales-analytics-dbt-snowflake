
WITH cleaned_product AS (
    SELECT
        NULLIF(TRIM(cd_product), '') AS cd_product,

        NULLIF(
            TRIM(
                REGEXP_REPLACE(
                    INITCAP(lib_product),
                    '[[:space:]]+',
                    ' '
                )
            ),
            ''
        ) AS lib_product,

        TRY_TO_DECIMAL(
            NULLIF(
                REPLACE(
                    REGEXP_REPLACE(
                        TRIM(TO_VARCHAR(price_product)),
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
        ) AS price_product,

        NULLIF(TRIM(cd_subcategory), '') AS cd_subcategory,

        INGESTED_AT

    FROM {{ source('bronze', 'product') }}
)

SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY cd_product
        ORDER BY
            CASE
                WHEN INGESTED_AT IS NOT NULL
                    AND cd_product IS NOT NULL
                    AND lib_product IS NOT NULL
                    AND price_product IS NOT NULL
                    AND cd_subcategory IS NOT NULL
                THEN 0

                WHEN INGESTED_AT IS NULL THEN 1

                ELSE 2
            END,

            INGESTED_AT DESC NULLS LAST,
            lib_product
    ) AS rn

FROM cleaned_product
