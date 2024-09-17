{{ config(
    tags=['normalized', 'xero', 'bank_transfers']
) }}

WITH bank_transfers_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BankTransferID') AS bank_transfer_id,
        JSON_VALUE(data, '$.FromBankAccount.AccountID') AS from_bank_account_id,
        JSON_VALUE(data, '$.FromBankAccount.Code') AS from_bank_account_code,
        JSON_VALUE(data, '$.FromBankAccount.Name') AS from_bank_account_name,
        JSON_VALUE(data, '$.ToBankAccount.AccountID') AS to_bank_account_id,
        JSON_VALUE(data, '$.ToBankAccount.Code') AS to_bank_account_code,
        JSON_VALUE(data, '$.ToBankAccount.Name') AS to_bank_account_name,
        CAST(JSON_VALUE(data, '$.Amount') AS NUMERIC) AS amount,
        TIMESTAMP_MILLIS(
            CAST(
                SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS date,
        CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        JSON_VALUE(data, '$.FromBankTransactionID') AS from_bank_transaction_id,
        JSON_VALUE(data, '$.ToBankTransactionID') AS to_bank_transaction_id,
        CAST(JSON_VALUE(data, '$.FromIsReconciled') AS BOOL) AS from_is_reconciled,
        CAST(JSON_VALUE(data, '$.ToIsReconciled') AS BOOL) AS to_is_reconciled,
        JSON_VALUE(data, '$.Reference') AS reference,
        CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments,
        TIMESTAMP_MILLIS(
            CAST(
                SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.CreatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS created_date_utc
    FROM 
        {{ source('raw', 'xero_bank_transfers') }}
)

SELECT
    ingestion_time,
    bank_transfer_id,
    from_bank_account_id,
    from_bank_account_code,
    from_bank_account_name,
    to_bank_account_id,
    to_bank_account_code,
    to_bank_account_name,    
    amount,
    date,
    currency_rate,
    from_bank_transaction_id,
    to_bank_transaction_id,
    from_is_reconciled,
    to_is_reconciled,
    reference,
    has_attachments,
    created_date_utc
FROM 
    bank_transfers_raw