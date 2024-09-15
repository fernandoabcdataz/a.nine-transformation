{{ config(
    tags=['normalized', 'xero', 'accounts']
) }}

WITH accounts_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.AccountID') AS account_id,
        JSON_VALUE(data, '$.Code') AS code,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.TaxType') AS tax_type,
        CAST(JSON_VALUE(data, '$.EnablePaymentsToAccount') AS BOOL) AS enable_payments_to_account,
        JSON_VALUE(data, '$.BankAccountNumber') AS bank_account_number,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        JSON_VALUE(data, '$.Description') AS description,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.SystemAccount') AS system_account
    FROM 
        {{ source('raw', 'xero_accounts') }}
)

SELECT
    ingestion_time,
    account_id,
    code,
    name,
    type,
    tax_type,
    enable_payments_to_account,
    bank_account_number,
    currency_code,
    description,
    status,
    system_account
FROM 
    accounts_raw