{{ config(
    tags=['normalized', 'xero', 'credit_note_line_items']
) }}

WITH credit_note_line_items_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.CreditNoteID') AS credit_note_id,
        line_item.value.Description AS description,
        CAST(line_item.value.UnitAmount AS NUMERIC) AS unit_amount,
        line_item.value.TaxType AS tax_type,
        CAST(line_item.value.TaxAmount AS NUMERIC) AS tax_amount,
        CAST(line_item.value.LineAmount AS NUMERIC) AS line_amount,
        JSON_VALUE(line_item.value, '$.AccountCode') AS account_code,
        JSON_VALUE(line_item.value, '$.AccountId') AS account_id,
        CAST(line_item.value.Quantity AS NUMERIC) AS quantity
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
    quantity
FROM 
    credit_note_line_items_raw