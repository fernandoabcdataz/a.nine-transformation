{{ config(
    tags=['normalized', 'xero', 'credit_notes', 'credit_note__allocations']
) }}

WITH credit_note_allocations_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.CreditNoteID') AS credit_note_id,
        JSON_VALUE(allocation, '$.AllocationID') AS allocation_id,
        SAFE_CAST(JSON_VALUE(allocation, '$.Amount') AS NUMERIC) AS amount,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS allocation_date,
        JSON_VALUE(allocation, '$.Invoice.InvoiceID') AS invoice_id,
        JSON_VALUE(allocation, '$.Invoice.InvoiceNumber') AS invoice_number
    FROM 
        {{ source('raw', 'xero_credit_notes') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Allocations')) AS allocation
)

SELECT
    ingestion_time,
    credit_note_id,
    allocation_id,
    amount,
    allocation_date,
    invoice_id,
    invoice_number
FROM 
    credit_note_allocations_raw