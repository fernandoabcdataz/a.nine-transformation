{{ config(
    tags=['normalized', 'xero', 'batch_payment_payments']
) }}

WITH batch_payment_payments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BatchPaymentID') AS batch_payment_id,
        JSON_VALUE(payment, '$.Invoice.InvoiceID') AS invoice_id,
        JSON_VALUE(payment, '$.PaymentID') AS payment_id,
        JSON_VALUE(payment, '$.BankAccountNumber') AS bank_account_number,
        JSON_VALUE(payment, '$.Particulars') AS payment_particulars,
        JSON_VALUE(payment, '$.Code') AS payment_code,
        JSON_VALUE(payment, '$.Reference') AS payment_reference,
        CAST(JSON_VALUE(payment, '$.Amount') AS NUMERIC) AS amount
    FROM 
        {{ source('raw', 'xero_batch_payments') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.Payments')) AS payment
)

SELECT
    ingestion_time,
    batch_payment_id,
    invoice_id,
    payment_id,
    bank_account_number,
    payment_particulars,
    payment_code,
    payment_reference,
    amount
FROM 
    batch_payment_payments_raw