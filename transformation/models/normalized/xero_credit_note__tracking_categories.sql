{{ config(
    tags=['normalized', 'xero', 'credit_note_tracking_categories']
) }}

WITH credit_note_tracking_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.CreditNoteID') AS credit_note_id,
        tracking.value.Name AS tracking_category_name,
        tracking.value.Option AS tracking_option_name,
        tracking.value.TrackingCategoryID AS tracking_category_id,
        tracking.value.TrackingOptionID AS tracking_option_id
    FROM 
        {{ source('raw', 'xero_credit_notes') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item,
        UNNEST(JSON_EXTRACT_ARRAY(line_item.value, '$.Tracking')) AS tracking
)

SELECT
    ingestion_time,
    credit_note_id,
    tracking_category_name,
    tracking_option_name,
    tracking_category_id,
    tracking_option_id
FROM 
    credit_note_tracking_raw