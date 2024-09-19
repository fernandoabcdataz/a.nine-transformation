{{ config(
    tags=['normalized', 'xero', 'tracking_category']
) }}

WITH tracking_options_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.TrackingCategoryID') AS tracking_category_id,
        JSON_VALUE(option, '$.TrackingOptionID') AS tracking_option_id,
        JSON_VALUE(option, '$.Name') AS name,
        JSON_VALUE(option, '$.Status') AS status
    FROM 
        {{ source('raw', 'xero_tracking_categories') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Options')) AS option
)

SELECT
    tracking_category_id,
    tracking_option_id,
    name,
    status
FROM 
    tracking_options_raw