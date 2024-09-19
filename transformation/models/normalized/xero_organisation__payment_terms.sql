{{ config(
    tags=['normalized', 'xero', 'organisation']
) }}

WITH payment_terms_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.OrganisationID') AS organisation_id,
        payment_term_type AS term_type,
        SAFE_CAST(payment_term.value.Day AS INT64) AS day,
        payment_term.value.Type AS type
    FROM 
        {{ source('raw', 'xero_organisation') }},
        UNNEST([STRUCT('Bills' AS payment_term_type, JSON_EXTRACT_OBJECT(data, '$.PaymentTerms.Bills') AS value),
               STRUCT('Sales' AS payment_term_type, JSON_EXTRACT_OBJECT(data, '$.PaymentTerms.Sales') AS value)]) AS payment_term
)

SELECT
    ingestion_time,
    organisation_id,
    term_type,
    day,
    type
FROM 
    payment_terms_raw