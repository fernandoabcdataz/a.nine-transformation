{{ config(
    tags=['normalized', 'xero', 'invoices']
) }}

WITH invoices_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.InvoiceID') AS invoice_id,
        JSON_VALUE(data, '$.InvoiceNumber') AS invoice_number,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        TIMESTAMP_MILLIS(CAST(REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)(?:[+-]\d+)?\)/') AS INT64)) AS invoice_date,
        TIMESTAMP_MILLIS(CAST(REGEXP_EXTRACT(JSON_VALUE(data, '$.DueDate'), r'/Date\((\d+)(?:[+-]\d+)?\)/') AS INT64)) AS due_date,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS FLOAT64) AS sub_total,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS FLOAT64) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS FLOAT64) AS total,
        SAFE_CAST(JSON_VALUE(data, '$.AmountDue') AS FLOAT64) AS amount_due,
        SAFE_CAST(JSON_VALUE(data, '$.AmountPaid') AS FLOAT64) AS amount_paid,
        SAFE_CAST(JSON_VALUE(data, '$.AmountCredited') AS FLOAT64) AS amount_credited,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        JSON_EXTRACT_ARRAY(data, '$.LineItems') AS line_items_array
    FROM 
        {{ source('raw', 'xero_invoices') }}
    WHERE 
        JSON_VALUE(data, '$.Type') = 'ACCREC'  -- Only Sales Invoices
)

SELECT
    ingestion_time,
    invoice_id,
    invoice_number,
    type,
    status,
    contact_id,
    invoice_date,
    due_date,
    sub_total,
    total_tax,
    total,
    amount_due,
    amount_paid,
    amount_credited,
    currency_code,
    line_items_array
FROM 
    invoices_raw