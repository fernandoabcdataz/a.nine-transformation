{{ config(
    tags=['normalized', 'xero', 'contacts', 'contact__phones']
) }}

WITH contact_phones_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(phone, '$.PhoneType') AS phone_type,
        JSON_VALUE(phone, '$.PhoneNumber') AS phone_number,
        JSON_VALUE(phone, '$.PhoneAreaCode') AS phone_area_code,
        JSON_VALUE(phone, '$.PhoneCountryCode') AS phone_country_code
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