{{ config(
    tags=['normalized', 'xero', 'journal_line_tracking_categories']
) }}

WITH tracking_categories_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.JournalID') AS journal_id,
        JSON_VALUE(journal_line.value, '$.JournalLineID') AS journal_line_id,
        tracking_category.value.Name AS tracking_category_name,
        tracking_category.value.Option AS tracking_option_name,
        tracking_category.value.TrackingCategoryID AS tracking_category_id,
        tracking_category.value.TrackingOptionID AS tracking_option_id
    FROM 
        {{ source('raw', 'xero_journals') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.JournalLines')) AS journal_line,
        UNNEST(JSON_EXTRACT_ARRAY(journal_line.value, '$.TrackingCategories')) AS tracking_category
)

SELECT
    ingestion_time,
    journal_id,
    journal_line_id,
    tracking_category_name,
    tracking_option_name,
    tracking_category_id,
    tracking_option_id
FROM 
    tracking_categories_raw