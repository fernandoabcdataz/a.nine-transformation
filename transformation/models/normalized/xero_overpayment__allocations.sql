{{ config(
    tags=['normalized', 'xero', 'overpayments']
) }}

WITH allocations_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.OverpaymentID') AS overpayment_id,
        allocation.value.AllocationID AS allocation_id,
        allocation.value.Amount AS allocation_amount,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(allocation.value.Date, r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS allocation_date,
        allocation.value.Invoice.InvoiceID AS invoice_id,
        allocation.value.Invoice.InvoiceNumber AS invoice_number
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