{{ config(
    tags=['normalized', 'xero', 'organisation']
) }}

WITH external_links_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.OrganisationID') AS organisation_id,
        external_link.value.LinkType AS link_type,
        external_link.value.Url AS url
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