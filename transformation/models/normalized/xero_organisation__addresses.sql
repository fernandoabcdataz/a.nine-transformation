{{ config(
    tags=['normalized', 'xero', 'organisation']
) }}

WITH addresses_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.OrganisationID') AS organisation_id,
        address.value.AddressType AS address_type,
        address.value.AddressLine1 AS address_line1,
        address.value.AddressLine2 AS address_line2,
        address.value.City AS city,
        address.value.PostalCode AS postal_code,
        address.value.Country AS country,
        address.value.AttentionTo AS attention_to
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