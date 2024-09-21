{{ config(
    tags=['normalized', 'xero', 'budgets', 'budget__lines']
) }}

WITH budget_lines_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BudgetID') AS budget_id,
        budget_line.value.AccountID AS account_id,
        budget_line.value.AccountCode AS account_code
    FROM 
        {{ source('raw', 'xero_budgets') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.BudgetLines')) AS budget_line
)

SELECT
    ingestion_time,
    budget_id,
    account_id,
    account_code
FROM 
    budget_lines_raw