{{ config(
    tags=['normalized', 'xero', 'bank_transactions']
) }}

WITH bank_transactions_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.BankTransactionID') AS bank_transaction_id,
        JSON_VALUE(data, '$.BankTransactionNumber') AS bank_transaction_number,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name,
        JSON_VALUE(data, '$.BankAccount.BankAccountID') AS bank_account_id,
        JSON_VALUE(data, '$.BankAccount.Code') AS bank_account_code,
        JSON_VALUE(data, '$.BankAccount.Name') AS bank_account_name,
        SAFE_CAST(JSON_VALUE(data, '$.IsReconciled') AS BOOL) AS is_reconciled,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS date,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        JSON_VALUE(data, '$.CurrencyRate') AS currency_rate,
        JSON_VALUE(data, '$.Url') AS url,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.Type') AS type,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS sub_total,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        SAFE_CAST(JSON_VALUE(data, '$.PrepaymentID') AS NUMERIC) AS prepayment_id,
        SAFE_CAST(JSON_VALUE(data, '$.OverpaymentID') AS NUMERIC) AS overpayment_id,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        SAFE_CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments
    FROM 
        {{ source('raw', 'xero_bank_transactions') }}
)

SELECT
    ingestion_time,
    bank_transaction_id,
    bank_transaction_number,
    contact_id,
    contact_name,
    bank_account_id,
    bank_account_code,
    bank_account_name,
    is_reconciled,
    date,
    line_amount_types,
    reference,
    currency_code,
    currency_rate,
    url,
    status,
    type,
    sub_total,
    total_tax,
    total,
    prepayment_id,
    overpayment_id,
    updated_date_utc,
    has_attachments
FROM 
    bank_transactions_raw