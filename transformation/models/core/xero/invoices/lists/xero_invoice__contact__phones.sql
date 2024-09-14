{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        Contact.ContactID AS contact_id,
        phone_value,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices')}},
        UNNEST(Contact.Phones) AS phone_value
    WHERE
        Contact.ContactID IS NOT NULL
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    CAST(contact_id AS STRING) AS contact_id,
    -- Parse phone_value JSON string
    CAST(JSON_VALUE(phone_value, '$.PhoneType') AS STRING) AS phone_type,
    CAST(JSON_VALUE(phone_value, '$.PhoneNumber') AS STRING) AS phone_number,
    CAST(JSON_VALUE(phone_value, '$.PhoneAreaCode') AS STRING) AS phone_area_code,
    CAST(JSON_VALUE(phone_value, '$.PhoneCountryCode') AS STRING) AS phone_country_code,
    ingestion_time
FROM
    source