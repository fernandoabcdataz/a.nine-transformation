{{ config(
    tags=['normalized', 'xero', 'organisation', 'organisation__external_links']
) }}

WITH external_links_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.OrganisationID') AS organisation_id,
        JSON_VALUE(external_link, '$.LinkType') AS link_type,
        JSON_VALUE(external_link, '$.Url') AS url
    FROM 
        {{ source('raw', 'xero_organisation') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.ExternalLinks')) AS external_link
)

SELECT
    ingestion_time,
    organisation_id,
    link_type,
    url
FROM 
    external_links_raw