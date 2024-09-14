{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'payments', 'hourly']
) }}

WITH source AS (
    SELECT
        PaymentID AS payment_id,
        Invoice.InvoiceID AS invoice_id,
        line_item AS line_item_value
    FROM
        {{ source('raw', 'xero_payments') }},
        UNNEST(Invoice.LineItems) AS line_item
    WHERE
        Invoice.InvoiceID IS NOT NULL
)

SELECT
    *
FROM
    source