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
        JSON_VALUE(data, '$.BankAccountNumber') AS bank_account_number,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.Description') AS description,
        JSON_VALUE(data, '$.BankAccountType') AS bank_account_type,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        JSON_VALUE(data, '$.TaxType') AS tax_type,
        CAST(JSON_VALUE(data, '$.EnablePaymentsToAccount') AS BOOL) AS enable_payments_to_account,
        CAST(JSON_VALUE(data, '$.ShowInExpenseClaims') AS BOOL) AS show_in_expense_claims,
        JSON_VALUE(data, '$.Class') AS class,
        JSON_VALUE(data, '$.SystemAccount') AS system_account,
        JSON_VALUE(data, '$.ReportingCode') AS reporting_code,
        JSON_VALUE(data, '$.ReportingCodeName') AS reporting_code_name,
        CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments,
        CAST(JSON_VALUE(data, '$.AddToWatchlist') AS BOOL) AS add_to_watchlist,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_accounts') }}
)

SELECT
    ingestion_time,
    account_id,
    code,
    name,
    type,
    bank_account_number,
    status,
    description,
    bank_account_type,
    currency_code,
    tax_type,
    enable_payments_to_account,
    show_in_expense_claims,
    class,
    system_account,
    reporting_code,
    reporting_code_name,
    has_attachments,
    add_to_watchlist,
    updated_date_utc
FROM 
    accounts_raw