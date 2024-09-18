{{ config(
    tags=['normalized', 'xero', 'currencies']
) }}

WITH currencies_raw AS (
    SELECT
        ingestion_time,
        currency.value.Code AS code,
        currency.value.Description AS description
    FROM 
        {{ source('raw', 'xero_currencies') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Currencies')) AS currency
)

SELECT
    ingestion_time,
    code,
    description
FROM 
    currencies_raw