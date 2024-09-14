{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'payments', 'hourly']
) }}

WITH source AS (
    SELECT
        PaymentID AS payment_id,
        Invoice.InvoiceID AS invoice_id,
        Invoice.Contact.ContactID AS contact_id,
        contact_person
    FROM
        {{ source('raw', 'xero_payments')}},
        UNNEST(Invoice.Contact.ContactPersons) AS contact_person
    WHERE
        Invoice.Contact.ContactID IS NOT NULL
)

SELECT
    *
FROM
    source