{{ config(
    tags=['normalized', 'xero', 'payments', 'hourly']
) }}

WITH payments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.PaymentID') AS payment_id,
        JSON_VALUE(data, '$.Invoice.InvoiceID') AS invoice_id,
        JSON_VALUE(data, '$.Account.AccountID') AS account_id,
        -- extract milliseconds safely
        SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)(?:[+-]\d+)?\)/') AS date_millis_str,
        JSON_VALUE(data, '$.Amount') AS amount_str,
        JSON_VALUE(data, '$.CurrencyRate') AS currency_rate_str,
        JSON_VALUE(data, '$.PaymentType') AS payment_type,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.IsReconciled') AS is_reconciled_str,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code
    FROM 
        {{ source('raw', 'xero_payments') }}
)

SELECT
    ingestion_time,
    payment_id,
    invoice_id,
    account_id,
    -- safely convert milliseconds to timestamp, return null if conversion fails
    CASE 
        WHEN SAFE_CAST(date_millis_str AS INT64) IS NOT NULL THEN TIMESTAMP_MILLIS(SAFE_CAST(date_millis_str AS INT64))
        ELSE NULL
    END AS payment_date,
    -- safely cast amount and currency rate
    SAFE_CAST(amount_str AS FLOAT64) AS amount,
    SAFE_CAST(currency_rate_str AS FLOAT64) AS currency_rate,
    payment_type,
    status,
    reference,
    -- safely cast is_reconciled
    SAFE_CAST(is_reconciled_str AS BOOL) AS is_reconciled,
    currency_code
FROM
    payments_raw