{{ config(
    tags=['normalized', 'xero', 'purchase_order_line_items']
) }}

WITH line_items_raw AS (
    SELECT
        JSON_VALUE(data, '$.PurchaseOrderID') AS purchase_order_id,
        JSON_VALUE(line_item.value, '$.LineItemID') AS line_item_id,
        JSON_VALUE(line_item.value, '$.Description') AS description,
        SAFE_CAST(JSON_VALUE(line_item.value, '$.Quantity') AS NUMERIC) AS quantity,
        SAFE_CAST(JSON_VALUE(line_item.value, '$.UnitAmount') AS NUMERIC) AS unit_amount,
        JSON_VALUE(line_item.value, '$.ItemCode') AS item_code,
        JSON_VALUE(line_item.value, '$.AccountCode') AS account_code,
        JSON_VALUE(line_item.value, '$.TaxType') AS tax_type,
        SAFE_CAST(JSON_VALUE(line_item.value, '$.TaxAmount') AS NUMERIC) AS tax_amount,
        SAFE_CAST(JSON_VALUE(line_item.value, '$.LineAmount') AS NUMERIC) AS line_amount,
        SAFE_CAST(JSON_VALUE(line_item.value, '$.DiscountRate') AS NUMERIC) AS discount_rate,
        JSON_VALUE(line_item.value, '$.TrackingCategory') AS tracking_category
    FROM 
        {{ source('raw', 'xero_purchase_orders') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item
)

SELECT
    purchase_order_id,
    line_item_id,
    description,
    quantity,
    unit_amount,
    item_code,
    account_code,
    tax_type,
    tax_amount,
    line_amount,
    discount_rate,
    tracking_category,
    ingestion_time
FROM 
    line_items_raw