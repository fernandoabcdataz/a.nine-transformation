{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        Contact.ContactID AS contact_id,
        contact_group_value,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices')}},
        UNNEST(Contact.ContactGroups) AS contact_group_value
    WHERE
        Contact.ContactID IS NOT NULL
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    CAST(contact_id AS STRING) AS contact_id,
    -- Parse contact_group_value JSON string
    CAST(JSON_VALUE(contact_group_value, '$.Name') AS STRING) AS group_name,
    CAST(JSON_VALUE(contact_group_value, '$.ContactGroupID') AS STRING) AS contact_group_id,
    CAST(JSON_VALUE(contact_group_value, '$.Status') AS STRING) AS status,
    ingestion_time
FROM
    source