{{ config(
    tags=['normalized', 'xero', 'contact_groups', 'contact_group__contacts']
) }}

WITH contact_group_contacts_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactGroupID') AS contact_group_id,
        JSON_VALUE(contact, '$.ContactID') AS contact_id,
        JSON_VALUE(contact, '$.Name') AS contact_name
    FROM 
        {{ source('raw', 'xero_contact_groups') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Contacts')) AS contact
)

SELECT
    ingestion_time,
    contact_group_id,
    contact_id,
    contact_name
FROM 
    contact_group_contacts_raw