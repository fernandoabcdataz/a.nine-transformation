{{ config(
    tags=['normalized', 'xero', 'contact_addresses']
) }}

WITH contact_addresses_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        address.value.AddressType AS address_type,
        address.value.AddressLine1 AS address_line1,
        address.value.AddressLine2 AS address_line2,
        address.value.City AS city,
        address.value.PostalCode AS postal_code,
        address.value.AttentionTo AS attention_to
    FROM 
        {{ source('raw', 'xero_contacts') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Addresses')) AS address
)

SELECT
    ingestion_time,
    contact_id,
    address_type,
    address_line1,
    address_line2,
    city,
    postal_code,
    attention_to
FROM 
    contact_addresses_raw