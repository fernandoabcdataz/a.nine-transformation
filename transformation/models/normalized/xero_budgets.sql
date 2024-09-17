{{ config(
    tags=['normalized', 'xero', 'budgets']
) }}

WITH budgets_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BudgetID') AS budget_id,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.Description') AS description,
        TIMESTAMP_MILLIS(
            CAST(
                SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_budgets') }}
)

SELECT
    ingestion_time,
    budget_id,
    type,
    description,
    updated_date_utc
FROM 
    budgets_raw