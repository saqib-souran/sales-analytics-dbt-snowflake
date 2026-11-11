
WITH cleaned_customer AS (
    SELECT
        NULLIF(TRIM(CUSTOMER_ID), '') AS CUSTOMER_ID,

        NULLIF(TRIM(REGEXP_REPLACE(
            INITCAP(LAST_NAME), '\\s+', ' '
        )), '') AS LAST_NAME,

        NULLIF(TRIM(REGEXP_REPLACE(
            INITCAP(FIRST_NAME), '\\s+', ' '
        )), '') AS FIRST_NAME,

        NULLIF(TRIM(REGEXP_REPLACE(
            INITCAP(ADDRESS), '\\s+', ' '
        )), '') AS ADDRESS,

        NULLIF(TRIM(REGEXP_REPLACE(
            POSTAL_CODE, '\\s+', ' '
        )), '') AS POSTAL_CODE,

        NULLIF(TRIM(REGEXP_REPLACE(
            CITY, '\\s+', ' '
        )), '') AS CITY,

        NULLIF(TRIM(REGEXP_REPLACE(
            INITCAP(COUNTRY), '\\s+', ' '
        )), '') AS COUNTRY,

        NULLIF(TRIM(CD_TYPE_CUSTOMER), '')
            AS CD_TYPE_CUSTOMER,

        INGESTED_AT

    FROM {{ source('bronze', 'customer') }}
)

SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_ID
        ORDER BY
            CASE
                WHEN INGESTED_AT IS NOT NULL
                    AND CUSTOMER_ID IS NOT NULL
                    AND LAST_NAME IS NOT NULL
                    AND POSTAL_CODE IS NOT NULL
                    AND CD_TYPE_CUSTOMER IS NOT NULL
                THEN 0

                WHEN INGESTED_AT IS NULL
                THEN 1

                ELSE 2
            END,

            INGESTED_AT DESC NULLS LAST,
            LAST_NAME,
            FIRST_NAME
    ) AS rn

FROM cleaned_customer
