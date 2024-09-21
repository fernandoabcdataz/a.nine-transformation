{{ config(
    tags=['normalized', 'xero', 'contacts', 'contact__addresses']
) }}

WITH contact_addresses_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(address, '$.AddressType') AS address_type,
        JSON_VALUE(address, '$.AddressLine1') AS address_line1,
        JSON_VALUE(address, '$.AddressLine2') AS address_line2,
        JSON_VALUE(address, '$.City') AS city,
        JSON_VALUE(address, '$.PostalCode') AS postal_code,
        JSON_VALUE(address, '$.AttentionTo') AS attention_to
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