{{ config(
    tags=['normalized', 'xero', 'organisation', 'organisation__phones']
) }}

WITH addresses_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.OrganisationID') AS organisation_id,
        JSON_VALUE(phone, '$.PhoneType') AS phone_type,
        JSON_VALUE(phone, '$.PhoneNumber') AS phone_number,
        JSON_VALUE(phone, '$.PhoneAreaCode') AS phone_area_code,
        JSON_VALUE(phone, '$.PhoneCountryCode') AS phone_country_code
    FROM 
        {{ source('raw', 'xero_organisation') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Phones')) AS phone
)

SELECT
    ingestion_time,
    organisation_id,
    phone_type,
    phone_number,
    phone_area_code,
    phone_country_code
FROM 
    addresses_raw