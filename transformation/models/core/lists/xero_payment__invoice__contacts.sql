{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'payments', 'hourly']
) }}

WITH source AS (
    SELECT
        PaymentID AS payment_id,
        Invoice.InvoiceID AS invoice_id,
        Invoice.Contact.ContactID AS contact_id,
        Invoice.Contact.Name AS contact_name,
        Invoice.Contact.HasValidationErrors AS contact_has_validation_errors
    FROM
        {{ source('raw', 'xero_payments')}}
    WHERE
        Invoice.Contact.ContactID IS NOT NULL
)

SELECT
    *
FROM
    source