{{ config(
    tags=['normalized', 'xero', 'batch_payments', 'batch_payment__payments']
) }}

WITH batch_payment_payments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BatchPaymentID') AS batch_payment_id,
        JSON_VALUE(payment, '$.Invoice.InvoiceID') AS invoice_id,
        JSON_VALUE(payment, '$.PaymentID') AS payment_id,
        JSON_QUERY_ARRAY(payment, '$.Invoice') AS invoice,
        JSON_VALUE(payment, '$.BankAccountNumber') AS bank_account_number,
        JSON_VALUE(payment, '$.Particulars') AS particulars,
        JSON_VALUE(payment, '$.Code') AS code,
        JSON_VALUE(payment, '$.Reference') AS reference,
        JSON_VALUE(payment, '$.Details') AS details,
        SAFE_CAST(JSON_VALUE(payment, '$.Amount') AS NUMERIC) AS amount
    FROM 
        {{ source('raw', 'xero_batch_payments') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Payments')) AS payment
)

SELECT
    ingestion_time,
    batch_payment_id,
    invoice_id,
    payment_id,
    invoice,
    bank_account_number,
    particulars,
    code,
    reference,
    details,
    amount
FROM 
    batch_payment_payments_raw