{{ config(
    tags=['normalized', 'xero', 'contact_phones']
) }}

WITH contact_phones_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        phone.value.PhoneType AS phone_type,
        phone.value.PhoneNumber AS phone_number,
        phone.value.PhoneAreaCode AS phone_area_code,
        phone.value.PhoneCountryCode AS phone_country_code
    FROM 
        {{ source('raw', 'xero_contacts') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Phones')) AS phone
)

SELECT
    ingestion_time,
    contact_id,
    phone_type,
    phone_number,
    phone_area_code,
    phone_country_code
FROM 
    contact_phones_raw