{{ config(
    tags=['normalized', 'xero', 'repeating_invoice_tracking_categories']
) }}

WITH tracking_categories_raw AS (
    SELECT
        JSON_VALUE(data, '$.RepeatingInvoiceID') AS repeating_invoice_id,
        JSON_VALUE(line_item.value, '$.LineItemID') AS line_item_id,
        JSON_VALUE(tracking.value, '$.TrackingCategoryID') AS tracking_category_id,
        JSON_VALUE(tracking.value, '$.TrackingOptionID') AS tracking_option_id,
        JSON_VALUE(tracking.value, '$.Name') AS category_name,
        JSON_VALUE(tracking.value, '$.Option') AS option_name
    FROM 
        {{ source('raw', 'xero_repeating_invoices') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item,
        UNNEST(JSON_EXTRACT_ARRAY(line_item.value, '$.Tracking')) AS tracking
)

SELECT
    repeating_invoice_id,
    line_item_id,
    tracking_category_id,
    tracking_option_id,
    category_name,
    option_name
FROM 
    tracking_categories_raw