{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        line_item_value,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices') }},
        UNNEST(LineItems) AS line_item_value
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    -- Parse line_item_value JSON string
    CAST(JSON_VALUE(line_item_value, '$.Description') AS STRING) AS description,
    CAST(JSON_VALUE(line_item_value, '$.Quantity') AS FLOAT64) AS quantity,
    CAST(JSON_VALUE(line_item_value, '$.UnitAmount') AS FLOAT64) AS unit_amount,
    CAST(JSON_VALUE(line_item_value, '$.AccountCode') AS STRING) AS account_code,
    CAST(JSON_VALUE(line_item_value, '$.TaxType') AS STRING) AS tax_type,
    CAST(JSON_VALUE(line_item_value, '$.LineItemID') AS STRING) AS line_item_id,
    CAST(JSON_VALUE(line_item_value, '$.ItemCode') AS STRING) AS item_code,
    CAST(JSON_VALUE(line_item_value, '$.TaxAmount') AS FLOAT64) AS tax_amount,
    CAST(JSON_VALUE(line_item_value, '$.LineAmount') AS FLOAT64) AS line_amount,
    CAST(JSON_VALUE(line_item_value, '$.DiscountRate') AS FLOAT64) AS discount_rate,
    ingestion_time
FROM
    source