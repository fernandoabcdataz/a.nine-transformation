{{ config(
    tags=['normalized', 'xero', 'budget_balances']
) }}

WITH budget_balances_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BudgetID') AS budget_id,
        JSON_VALUE(budget_line, '$.AccountID') AS account_id,
        balance.value.Period AS period,
        --CAST(balance.value.Amount AS NUMERIC) AS amount,
        balance.value.Amount AS amount,
        balance.value.UnitAmount AS unit_amount
    FROM 
        {{ source('raw', 'xero_budgets') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.BudgetLines')) AS budget_line,
        UNNEST(JSON_EXTRACT_ARRAY(budget_line, '$.BudgetBalances')) AS balance
)

SELECT
    ingestion_time,
    budget_id,
    account_id,
    period,
    amount,
    unit_amount
FROM 
    budget_balances_raw