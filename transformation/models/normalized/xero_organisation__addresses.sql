{{ config(
    tags=['normalized', 'xero', 'organisation', 'organisation__addresses']
) }}

WITH addresses_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.OrganisationID') AS organisation_id,
        JSON_VALUE(address, '$.AddressType') AS address_type,
        JSON_VALUE(address, '$.AddressLine1') AS address_line1,
        JSON_VALUE(address, '$.AddressLine2') AS address_line2,
        JSON_VALUE(address, '$.City') AS city,
        JSON_VALUE(address, '$.PostalCode') AS postal_code,
        JSON_VALUE(address, '$.Country') AS country,
        JSON_VALUE(address, '$.AttentionTo') AS attention_to
    FROM 
        {{ source('raw', 'xero_organisation') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Addresses')) AS address
)

SELECT
    ingestion_time,
    organisation_id,
    address_type,
    address_line1,
    address_line2,
    city,
    postal_code,
    country,
    attention_to
FROM 
    addresses_raw