{{ config(
    tags=['normalized', 'xero', 'currencies']
) }}

WITH currencies_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.Code') AS code,
        JSON_VALUE(data, '$.Description') AS description
    FROM 
        {{ source('raw', 'xero_currencies') }}
)

SELECT
    ingestion_time,
    code,
    description
FROM 
    currencies_raw