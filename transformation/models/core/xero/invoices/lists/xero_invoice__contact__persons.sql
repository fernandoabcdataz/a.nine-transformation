{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        Contact.ContactID AS contact_id,
        contact_person_value,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices')}},
        UNNEST(Contact.ContactPersons) AS contact_person_value
    WHERE
        Contact.ContactID IS NOT NULL
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    CAST(contact_id AS STRING) AS contact_id,
    -- Parse contact_person_value JSON string
    CAST(JSON_VALUE(contact_person_value, '$.FirstName') AS STRING) AS first_name,
    CAST(JSON_VALUE(contact_person_value, '$.LastName') AS STRING) AS last_name,
    CAST(JSON_VALUE(contact_person_value, '$.EmailAddress') AS STRING) AS email_address,
    CAST(JSON_VALUE(contact_person_value, '$.IncludeInEmails') AS BOOLEAN) AS include_in_emails,
    ingestion_time
FROM
    source