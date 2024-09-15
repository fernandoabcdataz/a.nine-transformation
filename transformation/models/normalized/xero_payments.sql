{{ config(
    tags=['normalized', 'xero', 'payments']
) }}

WITH payments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.PaymentID') AS payment_id,
        JSON_VALUE(data, '$.Invoice.InvoiceID') AS invoice_id,
        JSON_VALUE(data, '$.Account.AccountID') AS account_id,
        TIMESTAMP_MILLIS(CAST(REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)(?:[+-]\d+)?\)/') AS INT64)) AS payment_date,
        SAFE_CAST(JSON_VALUE(data, '$.Amount') AS FLOAT64) AS amount,
        SAFE_CAST(JSON_VALUE(data, '$.CurrencyRate') AS FLOAT64) AS currency_rate,
        JSON_VALUE(data, '$.PaymentType') AS payment_type,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.Reference') AS reference,
        CAST(JSON_VALUE(data, '$.IsReconciled') AS BOOL) AS is_reconciled,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code
    FROM 
        {{ source('raw', 'xero_payments') }}
)

SELECT
    ingestion_time,
    payment_id,
    invoice_id,
    account_id,
    payment_date,
    amount,
    currency_rate,
    payment_type,
    status,
    reference,
    is_reconciled,
    currency_code
FROM 
    payments_raw