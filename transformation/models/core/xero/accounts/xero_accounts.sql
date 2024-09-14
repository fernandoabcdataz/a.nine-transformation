{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'accounts']
) }}

WITH source AS (
    SELECT
        AccountID AS account_id,
        AddToWatchlist AS add_to_watchlist,
        BankAccountNumber AS bank_account_number,
        BankAccountType AS bank_account_type,
        Class AS class,
        Code AS code,
        CurrencyCode AS currency_code,
        Description AS description,
        EnablePaymentsToAccount AS enable_payments_to_account,
        HasAttachments AS has_attachments,
        Name AS name,
        ReportingCode AS reporting_code,
        ReportingCodeName AS reporting_code_name,
        ShowInExpenseClaims AS show_in_expense_claims,
        Status AS status,
        SystemAccount AS system_account,
        TaxType AS tax_type,
        Type AS type,
        -- parse UpdatedDateUTC from STRING to TIMESTAMP
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', UpdatedDateUTC) AS updated_date_utc,
        ingestion_time
    FROM
        {{ source('raw', 'xero_accounts')}}
)

SELECT
    CAST(account_id AS STRING) AS account_id,
    CAST(add_to_watchlist AS BOOLEAN) AS add_to_watchlist,
    -- bank account numbers may contain leading zeros; store as STRING
    CAST(bank_account_number AS STRING) AS bank_account_number,
    CAST(bank_account_type AS STRING) AS bank_account_type,
    CAST(class AS STRING) AS class,
    CAST(code AS STRING) AS code,
    CAST(currency_code AS STRING) AS currency_code,
    CAST(description AS STRING) AS description,
    CAST(enable_payments_to_account AS BOOLEAN) AS enable_payments_to_account,
    CAST(has_attachments AS BOOLEAN) AS has_attachments,
    CAST(name AS STRING) AS name,
    CAST(reporting_code AS STRING) AS reporting_code,
    CAST(reporting_code_name AS STRING) AS reporting_code_name,
    CAST(show_in_expense_claims AS BOOLEAN) AS show_in_expense_claims,
    CAST(status AS STRING) AS status,
    CAST(system_account AS STRING) AS system_account,
    CAST(tax_type AS STRING) AS tax_type,
    CAST(type AS STRING) AS type,
    updated_date_utc,
    ingestion_time
FROM
    source