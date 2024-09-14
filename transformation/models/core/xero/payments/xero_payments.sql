{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'payments', 'hourly']
) }}

WITH source AS (
    SELECT
        PaymentID AS payment_id,
        Account.AccountID AS account_id,
        Account.Code AS account_code,
        Amount,
        BankAmount,
        CurrencyRate,
        SAFE.PARSE_DATE('%Y-%m-%d', Date) AS date,
        HasAccount,
        HasValidationErrors,
        IsReconciled,
        PaymentType,
        Reference,
        Status,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', UpdatedDateUTC) AS updated_date_utc,
        ingestion_time
    FROM
      {{ source('raw', 'xero_payments')}}
)

SELECT
    *
FROM
    source