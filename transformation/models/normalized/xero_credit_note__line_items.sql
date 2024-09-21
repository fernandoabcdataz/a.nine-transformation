{{ config(
    tags=['normalized', 'xero', 'credit_notes', 'credit_note__line_items']
) }}

WITH credit_note_line_items_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.CreditNoteID') AS credit_note_id,
        JSON_VALUE(line_item, '$.Description') AS description,
        SAFE_CAST(JSON_VALUE(line_item, '$.UnitAmount') AS NUMERIC) AS unit_amount,
        JSON_VALUE(line_item, '$.TaxType') AS tax_type,
        SAFE_CAST(JSON_VALUE(line_item, '$.TaxAmount') AS NUMERIC) AS tax_amount,
        SAFE_CAST(JSON_VALUE(line_item, '$.LineAmount') AS NUMERIC) AS line_amount,
        JSON_VALUE(line_item, '$.AccountCode') AS account_code,
        JSON_VALUE(line_item, '$.AccountId') AS account_id,
        SAFE_CAST(JSON_VALUE(line_item, '$.Quantity') AS NUMERIC) AS quantity,
        JSON_VALUE(line_item, '$.Tracking') AS tracking
    FROM 
        {{ source('raw', 'xero_credit_notes') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item
)

SELECT
    ingestion_time,
    credit_note_id,
    description,
    unit_amount,
    tax_type,
    tax_amount,
    line_amount,
    account_code,
    account_id,
    quantity,
    tracking
FROM 
    credit_note_line_items_raw