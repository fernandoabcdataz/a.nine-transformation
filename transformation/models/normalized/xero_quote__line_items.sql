{{ config(
    tags=['normalized', 'xero', 'quotes']
) }}

WITH line_items_raw AS (
    SELECT
        JSON_VALUE(data, '$.QuoteID') AS quote_id,
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
        SAFE_CAST(JSON_VALUE(line_item.value, '$.DiscountAmount') AS NUMERIC) AS discount_amount
    FROM 
        {{ source('raw', 'xero_quotes') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item
)

SELECT
    quote_id,
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
    discount_amount
FROM 
    line_items_raw