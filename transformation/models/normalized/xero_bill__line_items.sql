{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices', 'bills', 'bill_line_items']
) }}

WITH bill_line_items_raw AS (
    SELECT
        ingestion_time,
        bill_id,
        JSON_VALUE(line_item, '$.LineItemID') AS line_item_id,
        JSON_VALUE(line_item, '$.Description') AS description,
        JSON_VALUE(line_item, '$.ItemCode') AS item_code,
        SAFE_CAST(JSON_VALUE(line_item, '$.Quantity') AS FLOAT64) AS quantity,
        SAFE_CAST(JSON_VALUE(line_item, '$.UnitAmount') AS FLOAT64) AS unit_amount,
        JSON_VALUE(line_item, '$.TaxType') AS tax_type,
        SAFE_CAST(JSON_VALUE(line_item, '$.TaxAmount') AS FLOAT64) AS tax_amount,
        SAFE_CAST(JSON_VALUE(line_item, '$.LineAmount') AS FLOAT64) AS line_amount,
        SAFE_CAST(JSON_VALUE(line_item, '$.DiscountRate') AS FLOAT64) AS discount_rate
    FROM 
        {{ source('raw', 'xero_invoices') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item
    WHERE 
        JSON_VALUE(data, '$.Type') = 'ACCPAY'  -- only bills
)

SELECT
    ingestion_time,
    bill_id,
    line_item_id,
    description,
    item_code,
    quantity,
    unit_amount,
    tax_type,
    tax_amount,
    line_amount,
    discount_rate
FROM 
    bill_line_items_raw