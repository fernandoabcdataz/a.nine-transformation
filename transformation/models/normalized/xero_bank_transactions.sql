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
        CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS sub_total,
        CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.BankTransactionNumber') AS bank_transaction_number,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        CAST(JSON_VALUE(data, '$.AddToWatchlist') AS BOOL) AS add_to_watchlist
    FROM 
        {{ source('raw', 'xero_bank_transactions') }}
),

line_items AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BankTransactionID') AS bank_transaction_id,
        JSON_VALUE(line_item, '$.Description') AS description,
        CAST(JSON_VALUE(line_item, '$.Quantity') AS NUMERIC) AS quantity,
        CAST(JSON_VALUE(line_item, '$.UnitAmount') AS NUMERIC) AS unit_amount,
        JSON_VALUE(line_item, '$.AccountCode') AS account_code,
        JSON_VALUE(line_item, '$.TaxType') AS tax_type,
        CAST(JSON_VALUE(line_item, '$.TaxAmount') AS NUMERIC) AS tax_amount
    FROM 
        {{ source('raw', 'xero_bank_transactions') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.LineItems')) AS line_item
)

SELECT
    bt.ingestion_time,
    bt.bank_transaction_id,
    bt.type,
    bt.contact_id,
    bt.contact_name,
    bt.date,
    bt.status,
    bt.currency_code,
    bt.bank_account_id,
    bt.bank_account_code,
    bt.bank_account_name,
    bt.sub_total,
    bt.total_tax,
    bt.total,
    bt.reference,
    bt.bank_transaction_number,
    bt.updated_date_utc,
    li.description,
    li.quantity,
    li.unit_amount,
    li.account_code,
    li.tax_type,
    li.tax_amount,
    bt.add_to_watchlist
FROM 
    bank_transactions_raw bt
LEFT JOIN 
    line_items li
ON 
    bt.bank_transaction_id = li.bank_transaction_id