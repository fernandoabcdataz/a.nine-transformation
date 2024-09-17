{{ config(
    tags=['normalized', 'xero', 'contact_persons']
) }}

WITH contact_persons_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        person.value.FirstName AS first_name,
        person.value.LastName AS last_name,
        person.value.EmailAddress AS email_address,
        SAFE_CAST(JSON_VALUE(person, '$.IncludeInEmails') AS BOOL) AS include_in_emails
    FROM 
        {{ source('raw', 'xero_contacts') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.ContactPersons')) AS person
)

SELECT
    ingestion_time,
    contact_id,
    first_name,
    last_name,
    email_address,
    include_in_emails
FROM 
    contact_persons_raw