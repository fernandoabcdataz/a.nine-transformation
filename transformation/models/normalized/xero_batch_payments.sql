{{ config(
    tags=['normalized', 'xero', 'batch_payments']
) }}

WITH batch_payments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.BatchPaymentID') AS batch_payment_id,
        JSON_VALUE(data, '$.Account.AccountID') AS account_id,        
        JSON_VALUE(data, '$.Particulars') AS particulars, -- NZ only fields
        JSON_VALUE(data, '$.Code') AS code, -- NZ only fields
        JSON_VALUE(data, '$.Reference') AS reference, -- NZ only fields
        -- JSON_VALUE(data, '$.Details') AS details, -- Non-NZ only fields
        JSON_VALUE(data, '$.Narrative') AS narrative, -- UK only fields
        TIMESTAMP_MILLIS(
            CAST(
                SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS date, -- parsing Date
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.Status') AS status,
        CAST(JSON_VALUE(data, '$.TotalAmount') AS NUMERIC) AS total_amount,
        CAST(JSON_VALUE(data, '$.IsReconciled') AS BOOL) AS is_reconciled,
        TIMESTAMP_MILLIS(
            CAST(
                SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc -- parsing UpdatedDateUTC
    FROM 
        {{ source('raw', 'xero_batch_payments') }}
)

SELECT
    ingestion_time,
    batch_payment_id,
    account_id,
    particulars,
    code,
    reference,
    -- details,
    narrative,
    date,
    type,
    status,
    total_amount,
    is_reconciled,
    updated_date_utc
FROM 
    batch_payments_raw