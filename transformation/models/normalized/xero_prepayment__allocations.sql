{{ config(
    tags=['normalized', 'xero', 'prepayment_allocations']
) }}

WITH allocations_raw AS (
    SELECT
        JSON_VALUE(data, '$.PrepaymentID') AS prepayment_id,
        SAFE_CAST(NULLIF(JSON_VALUE(allocation.value, '$.Amount'), '') AS NUMERIC) AS allocation_amount,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(allocation.value, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS allocation_date,
        JSON_VALUE(allocation.value, '$.Invoice.InvoiceID') AS invoice_id,
        JSON_VALUE(allocation.value, '$.Invoice.InvoiceNumber') AS invoice_number
    FROM 
        {{ source('raw', 'xero_prepayments') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Allocations')) AS allocation
)

SELECT
    prepayment_id,
    allocation_amount,
    allocation_date,
    invoice_id,
    invoice_number
FROM 
  allocations_raw