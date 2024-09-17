
{{ config(
    tags=['normalized', 'xero', 'contacts', 'addresses']
) }}

WITH addresses_raw AS (
    SELECT
        ce.ingestion_time,
        ce.contact_id,
        address.address_type,
        address.address_line1,
        address.city,
        address.postal_code,
        address.attention_to
    FROM 
        {{ ref('contacts_extension') }} ce,
        UNNEST(JSON_EXTRACT_ARRAY(ce.addresses)) AS address_json,
        UNNEST([STRUCT(
            JSON_VALUE(address_json, '$.AddressType') AS address_type,
            JSON_VALUE(address_json, '$.AddressLine1') AS address_line1,
            JSON_VALUE(address_json, '$.City') AS city,
            JSON_VALUE(address_json, '$.PostalCode') AS postal_code,
            JSON_VALUE(address_json, '$.AttentionTo') AS attention_to
        )]) AS address
    WHERE 
        ce.addresses IS NOT NULL
)

SELECT
    ingestion_time,
    contact_id,
    address_type,
    address_line1,
    city,
    postal_code,
    attention_to
FROM 
    addresses_raw