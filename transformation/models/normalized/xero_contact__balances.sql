{{ config(
    tags=['normalized', 'xero', 'contacts', 'contact__balances']
) }}

WITH contact_balances_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        CAST(JSON_VALUE(data, '$.Balances.AccountsReceivable.Outstanding') AS NUMERIC) AS accounts_receivable_outstanding,
        CAST(JSON_VALUE(data, '$.Balances.AccountsReceivable.Overdue') AS NUMERIC) AS accounts_receivable_overdue,
        CAST(JSON_VALUE(data, '$.Balances.AccountsPayable.Outstanding') AS NUMERIC) AS accounts_payable_outstanding,
        CAST(JSON_VALUE(data, '$.Balances.AccountsPayable.Overdue') AS NUMERIC) AS accounts_payable_overdue
    FROM 
        {{ source('raw', 'xero_contacts') }}
)

SELECT
    ingestion_time,
    contact_id,
    accounts_receivable_outstanding,
    accounts_receivable_overdue,
    accounts_payable_outstanding,
    accounts_payable_overdue
FROM 
    contact_balances_raw