{{ config(
    tags=['normalized', 'xero', 'bank_transactions']
) }}

WITH bank_transactions_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BankTransactionID') AS bank_transaction_id,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS date,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        JSON_VALUE(data, '$.BankAccount.BankAccountID') AS bank_account_id,
        JSON_VALUE(data, '$.BankAccount.Code') AS bank_account_code,
        JSON_VALUE(data, '$.BankAccount.Name') AS bank_account_name,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS sub_total,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.BankTransactionNumber') AS bank_transaction_number,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        SAFE_CAST(JSON_VALUE(data, '$.AddToWatchlist') AS BOOL) AS add_to_watchlist
    FROM 
        {{ source('raw', 'xero_bank_transactions') }}
)

SELECT
    ingestion_time,
    bank_transaction_id,
    add_to_watchlist,
    bank_account_code,
    bank_account_id,
    bank_account_name,
    bank_transaction_number,
    contact_id,
    contact_name,
    currency_code,
    date,
    reference,
    status,
    sub_total,
    total,
    total_tax,
    type,
    updated_date_utc
FROM 
    bank_transactions_raw