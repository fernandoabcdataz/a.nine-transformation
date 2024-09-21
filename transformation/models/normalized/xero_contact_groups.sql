{{ config(
    tags=['normalized', 'xero', 'contact_groups']
) }}

WITH contact_groups_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.ContactGroupID') AS contact_group_id,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.Status') AS status
    FROM 
        {{ source('raw', 'xero_contact_groups') }}
)

SELECT
    ingestion_time,
    contact_group_id,
    name,
    status
FROM 
    contact_groups_raw