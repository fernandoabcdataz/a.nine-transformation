
{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'contacts', 'phones']
) }}

WITH phones_raw AS (
    SELECT
        ce.ingestion_time,
        ce.contact_id,
        phone.phone_type,
        phone.phone_number,
        phone.phone_area_code,
        phone.phone_country_code
    FROM 
        {{ ref('contacts_extension') }} ce,
        UNNEST(JSON_EXTRACT_ARRAY(ce.phones)) AS phone_json,
        UNNEST([STRUCT(
            JSON_VALUE(phone_json, '$.PhoneType') AS phone_type,
            JSON_VALUE(phone_json, '$.PhoneNumber') AS phone_number,
            JSON_VALUE(phone_json, '$.PhoneAreaCode') AS phone_area_code,
            JSON_VALUE(phone_json, '$.PhoneCountryCode') AS phone_country_code
        )]) AS phone
    WHERE 
        ce.phones IS NOT NULL
)

SELECT
    ingestion_time,
    contact_id,
    phone_type,
    phone_number,
    phone_area_code,
    phone_country_code
FROM 
    phones_raw