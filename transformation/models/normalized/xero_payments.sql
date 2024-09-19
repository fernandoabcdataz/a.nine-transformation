{{ config(
    tags=['normalized', 'xero', 'payments']
) }}

WITH payments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.PaymentID') AS payment_id,
        JSON_VALUE(data, '$.Date') AS payment_date,
        SAFE_CAST(JSON_VALUE(data, '$.Amount') AS NUMERIC) AS amount,
        SAFE_CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        JSON_VALUE(data, '$.Reference') AS reference,
        SAFE_CAST(JSON_VALUE(data, '$.IsReconciled') AS BOOL) AS is_reconciled,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.PaymentType') AS payment_type,
        JSON_VALUE(data, '$.BatchPaymentID') AS batch_payment_id,
        SAFE_CAST(JSON_VALUE(data, '$.HasAccount') AS BOOL) AS has_account,
        SAFE_CAST(JSON_VALUE(data, '$.IsReconciled') AS BOOL) AS is_reconciled_payment,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        JSON_VALUE(data, '$.BatchPayment.BatchPaymentID') AS batch_payment_batch_payment_id,
        JSON_VALUE(data, '$.BatchPayment.Type') AS batch_payment_type,
        JSON_VALUE(data, '$.BatchPayment.Status') AS batch_payment_status,
        SAFE_CAST(JSON_VALUE(data, '$.BatchPayment.TotalAmount') AS NUMERIC) AS batch_payment_total_amount,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.BatchPayment.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS batch_payment_date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.BatchPayment.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS batch_payment_updated_date_utc,
        SAFE_CAST(JSON_VALUE(data, '$.BatchPayment.IsReconciled') AS BOOL) AS batch_payment_is_reconciled,
        JSON_VALUE(data, '$.BatchPayment.Account.AccountID') AS batch_payment_account_id,
        JSON_VALUE(data, '$.Account.AccountID') AS account_id,
        JSON_VALUE(data, '$.Account.Code') AS account_code,
        JSON_VALUE(data, '$.Invoice.Type') AS invoice_type,
        JSON_VALUE(data, '$.Invoice.InvoiceID') AS invoice_id,
        JSON_VALUE(data, '$.Invoice.InvoiceNumber') AS invoice_number,
        JSON_VALUE(data, '$.Invoice.Contact.ContactID') AS invoice_contact_id,
        JSON_VALUE(data, '$.Invoice.Contact.Name') AS invoice_contact_name,
        JSON_VALUE(data, '$.CreditNote.CreditNoteID') AS credit_note_id,
        JSON_VALUE(data, '$.Prepayments.PrepaymentID') AS prepayment_id,
        JSON_VALUE(data, '$.Overpayment.OverpaymentID') AS overpayment_id
    FROM 
        {{ source('raw', 'xero_payments') }}
)

SELECT
    payment_id,
    payment_date,
    amount,
    currency_rate,
    reference,
    is_reconciled,
    status,
    payment_type,
    batch_payment_id,
    has_account,
    is_reconciled_payment,
    updated_date_utc,
    batch_payment_batch_payment_id,
    batch_payment_type,
    batch_payment_status,
    batch_payment_total_amount,
    batch_payment_date,
    batch_payment_updated_date_utc,
    batch_payment_is_reconciled,
    batch_payment_account_id,
    account_id,
    account_code,
    invoice_type,
    invoice_id,
    invoice_number,
    invoice_contact_id,
    invoice_contact_name,
    credit_note_id,
    prepayment_id,
    overpayment_id,
    ingestion_time
FROM 
    payments_raw