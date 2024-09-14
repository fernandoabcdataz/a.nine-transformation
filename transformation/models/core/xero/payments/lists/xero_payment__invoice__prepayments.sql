{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'payments', 'hourly']
) }}

WITH source AS (
    SELECT
        PaymentID AS payment_id,
        Invoice.InvoiceID AS invoice_id,
        prepayment,
        ingestion_time
    FROM
        {{ source('raw', 'xero_payments')}},
        UNNEST(Invoice.Prepayments) AS prepayment
    WHERE
        Invoice.InvoiceID IS NOT NULL
)

SELECT
    *
FROM
    source