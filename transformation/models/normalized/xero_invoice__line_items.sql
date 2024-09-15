{{ config(
    tags=['normalized', 'xero', 'invoices', 'invoice_line_items']
) }}

WITH bill_line_items_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.InvoiceID') AS invoice_id,
        JSON_VALUE(line_item, '$.LineItemID') AS line_item_id,
        JSON_VALUE(line_item, '$.Description') AS description,
        JSON_VALUE(line_item, '$.ItemCode') AS item_code,
        JSON_VALUE(line_item, '$.AccountCode') AS account_code,
        JSON_VALUE(line_item, '$.AccountID') AS account_id,
        JSON_VALUE(line_item, '$.Tracking') AS tracking,
        SAFE_CAST(JSON_VALUE(line_item, '$.Quantity') AS FLOAT64) AS quantity,
        SAFE_CAST(JSON_VALUE(line_item, '$.UnitAmount') AS FLOAT64) AS unit_amount,
        SAFE_CAST(JSON_VALUE(line_item, '$.DiscountRate') AS FLOAT64) AS discount_rate,
        SAFE_CAST(JSON_VALUE(line_item, '$.DiscountAmount') AS FLOAT64) AS discount_amount,
        JSON_VALUE(line_item, '$.TaxType') AS tax_type,
        SAFE_CAST(JSON_VALUE(line_item, '$.TaxAmount') AS FLOAT64) AS tax_amount,
        SAFE_CAST(JSON_VALUE(line_item, '$.LineAmount') AS FLOAT64) AS line_amount
    FROM 
        {{ source('raw', 'xero_invoices') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item
    WHERE 
        JSON_VALUE(data, '$.Type') = 'ACCREC'  -- only sales invoices
)

SELECT
    ingestion_time,
    invoice_id,
    line_item_id,
    description,
    item_code,
    account_code,
    account_id,  -- Now included
    tracking,
    quantity,
    unit_amount,
    discount_rate,
    discount_amount,
    tax_type,
    tax_amount,
    line_amount
FROM 
    bill_line_items_raw