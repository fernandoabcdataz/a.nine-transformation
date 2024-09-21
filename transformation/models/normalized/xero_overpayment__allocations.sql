{{ config(
    tags=['normalized', 'xero', 'overpayments', 'overpayment__allocations']
) }}

WITH allocations_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.OverpaymentID') AS overpayment_id,
        JSON_VALUE(allocation, '$.AllocationID') AS allocation_id,
        JSON_VALUE(allocation, '$.Amount') AS allocation_amount,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(allocation, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS allocation_date,
        JSON_VALUE(allocation, '$.Invoice.InvoiceID') AS invoice_id,
        JSON_VALUE(allocation, '$.Invoice.InvoiceNumber') AS invoice_number
    FROM 
        {{ source('raw', 'xero_overpayments') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Allocations')) AS allocation
)

SELECT
    ingestion_time,
    overpayment_id,
    allocation_id,
    allocation_amount,
    allocation_date,
    invoice_id,
    invoice_number
FROM 
    allocations_raw