{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        overpayment_value,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices')}},
        UNNEST(Overpayments) AS overpayment_value
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    -- Parse overpayment_value JSON string
    CAST(JSON_VALUE(overpayment_value, '$.OverpaymentID') AS STRING) AS overpayment_id,
    CAST(JSON_VALUE(overpayment_value, '$.Date') AS STRING) AS date_string,
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(overpayment_value, '$.Date')) AS date,
    CAST(JSON_VALUE(overpayment_value, '$.Total') AS FLOAT64) AS total,
    CAST(JSON_VALUE(overpayment_value, '$.CurrencyRate') AS FLOAT64) AS currency_rate,
    CAST(JSON_VALUE(overpayment_value, '$.Status') AS STRING) AS status,
    ingestion_time
FROM
    source