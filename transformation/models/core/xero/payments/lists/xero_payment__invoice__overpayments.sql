{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'payments', 'hourly']
) }}

WITH source AS (
    SELECT
        PaymentID AS payment_id,
        Invoice.InvoiceID AS invoice_id,
        overpayment,
        ingestion_time
    FROM
        {{ source('raw', 'xero_payments')}},
        UNNEST(Invoice.Overpayments) AS overpayment
    WHERE
        Invoice.InvoiceID IS NOT NULL
)

SELECT
    *
FROM
    source