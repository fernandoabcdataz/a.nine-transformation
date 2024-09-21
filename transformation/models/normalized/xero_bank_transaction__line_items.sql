{{ config(
    tags=['normalized', 'xero', 'bank_transactions', 'bank_transaction__line_items']
) }}

WITH line_items_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BankTransactionID') AS bank_transaction_id,
        JSON_VALUE(line_item, '$.LineItemID') AS line_item_id,
        JSON_VALUE(line_item, '$.Description') AS description,
        SAFE_CAST(JSON_VALUE(line_item, '$.Quantity') AS NUMERIC) AS quantity,
        SAFE_CAST(JSON_VALUE(line_item, '$.UnitAmount') AS NUMERIC) AS unit_amount,
        JSON_VALUE(line_item, '$.AccountCode') AS account_code,
        JSON_VALUE(line_item, '$.ItemCode') AS item_code,
        JSON_VALUE(line_item, '$.TaxType') AS tax_type,
        SAFE_CAST(JSON_VALUE(line_item, '$.TaxAmount') AS NUMERIC) AS tax_amount,
        SAFE_CAST(JSON_VALUE(line_item, '$.LineAmount') AS NUMERIC) AS line_amount
    FROM 
        {{ source('raw', 'xero_bank_transactions') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item
)

SELECT
    ingestion_time,
    bank_transaction_id,
    line_item_id,
    description,
    quantity,
    unit_amount,
    account_code,
    item_code,
    tax_type,
    tax_amount,
    line_amount
FROM 
    line_items_raw