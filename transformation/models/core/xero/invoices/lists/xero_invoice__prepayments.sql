{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        prepayment_value,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices')}},
        UNNEST(Prepayments) AS prepayment_value
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    -- Parse prepayment_value JSON string
    CAST(JSON_VALUE(prepayment_value, '$.PrepaymentID') AS STRING) AS prepayment_id,
    CAST(JSON_VALUE(prepayment_value, '$.Date') AS STRING) AS date_string,
    SAFE.PARSE_DATE('%Y-%m-%d', JSON_VALUE(prepayment_value, '$.Date')) AS date,
    CAST(JSON_VALUE(prepayment_value, '$.Total') AS FLOAT64) AS total,
    CAST(JSON_VALUE(prepayment_value, '$.CurrencyRate') AS FLOAT64) AS currency_rate,
    CAST(JSON_VALUE(prepayment_value, '$.Status') AS STRING) AS status,
    ingestion_time
FROM
    source