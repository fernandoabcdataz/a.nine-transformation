{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        Contact.ContactID AS contact_id,
        address_value,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices')}},
        UNNEST(Contact.Addresses) AS address_value
    WHERE
        Contact.ContactID IS NOT NULL
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    CAST(contact_id AS STRING) AS contact_id,
    -- Parse address_value JSON string
    CAST(JSON_VALUE(address_value, '$.AddressType') AS STRING) AS address_type,
    CAST(JSON_VALUE(address_value, '$.AddressLine1') AS STRING) AS address_line1,
    CAST(JSON_VALUE(address_value, '$.AddressLine2') AS STRING) AS address_line2,
    CAST(JSON_VALUE(address_value, '$.City') AS STRING) AS city,
    CAST(JSON_VALUE(address_value, '$.Region') AS STRING) AS region,
    CAST(JSON_VALUE(address_value, '$.PostalCode') AS STRING) AS postal_code,
    CAST(JSON_VALUE(address_value, '$.Country') AS STRING) AS country,
    CAST(JSON_VALUE(address_value, '$.AttentionTo') AS STRING) AS attention_to,
    ingestion_time
FROM
    source