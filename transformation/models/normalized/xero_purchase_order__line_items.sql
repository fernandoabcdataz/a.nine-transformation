{{ config(
    tags=['normalized', 'xero', 'purchase_orders', 'purchase_order__line_items']
) }}

WITH line_items_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.PurchaseOrderID') AS purchase_order_id,
        JSON_VALUE(line_item, '$.LineItemID') AS line_item_id,
        JSON_VALUE(line_item, '$.Description') AS description,
        SAFE_CAST(JSON_VALUE(line_item, '$.Quantity') AS NUMERIC) AS quantity,
        SAFE_CAST(JSON_VALUE(line_item, '$.UnitAmount') AS NUMERIC) AS unit_amount,
        JSON_VALUE(line_item, '$.ItemCode') AS item_code,
        JSON_VALUE(line_item, '$.AccountCode') AS account_code,
        JSON_VALUE(line_item, '$.TaxType') AS tax_type,
        SAFE_CAST(JSON_VALUE(line_item, '$.DiscountRate') AS NUMERIC) AS discount_rate,
        SAFE_CAST(JSON_VALUE(line_item, '$.TaxAmount') AS NUMERIC) AS tax_amount,
        SAFE_CAST(JSON_VALUE(line_item, '$.LineAmount') AS NUMERIC) AS line_amount,
        JSON_QUERY_ARRAY(line_item, '$.Tracking') AS tracking
    FROM 
        {{ source('raw', 'xero_purchase_orders') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item
)

SELECT
    ingestion_time,
    purchase_order_id,
    line_item_id,
    description,
    quantity,
    unit_amount,
    item_code,
    account_code,
    tax_type,
    discount_rate,
    tax_amount,
    line_amount,
    tracking
FROM 
    line_items_raw