{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'payments', 'hourly']
) }}

WITH source AS (
    SELECT
        PaymentID AS payment_id,
        Invoice.InvoiceID AS invoice_id,
        Invoice.InvoiceNumber AS invoice_number,
        Invoice.Type AS invoice_type,
        Invoice.CurrencyCode AS invoice_currency_code,
        Invoice.HasErrors AS invoice_has_errors,
        Invoice.IsDiscounted AS invoice_is_discounted
    FROM
        {{ source('raw', 'xero_payments')}}
    WHERE
        Invoice.InvoiceID IS NOT NULL
)

SELECT
    *
FROM
    source