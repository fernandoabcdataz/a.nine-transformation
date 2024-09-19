{{ config(
    tags=['normalized', 'xero', 'tracking_categories']
) }}

WITH tracking_categories_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.TrackingCategoryID') AS tracking_category_id,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.Status') AS status
    FROM 
        {{ source('raw', 'xero_tracking_categories') }}
)

SELECT
    ingestion_time,
    tracking_category_id,
    name,
    status
FROM 
    tracking_categories_raw