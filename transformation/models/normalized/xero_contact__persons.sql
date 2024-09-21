{{ config(
    tags=['normalized', 'xero', 'contacts', 'contact__persons']
) }}

WITH contact_persons_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(person, '$.FirstName') AS first_name,
        JSON_VALUE(person, '$.LastName') AS last_name,
        JSON_VALUE(person, '$.EmailAddress') AS email_address,
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