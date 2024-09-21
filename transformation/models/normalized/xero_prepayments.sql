{{ config(
    tags=['normalized', 'xero', 'prepayments']
) }}

WITH prepayments_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.PrepaymentID') AS prepayment_id,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS date,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS subtotal,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        SAFE_CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        JSON_VALUE(data, '$.Reference') AS reference,
        SAFE_CAST(JSON_VALUE(data, '$.RemainingCredit') AS NUMERIC) AS remaining_credit,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.FullyPaidOnDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS fully_paid_on_date,
        JSON_VALUE(data, '$.Type') AS type,
        SAFE_CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments
    FROM 
        {{ source('raw', 'xero_prepayments') }}
)

SELECT
    ingestion_time,
    prepayment_id,
    contact_id,
    contact_name,
    date,
    status,
    line_amount_types,
    subtotal,
    total_tax,
    total,
    updated_date_utc,
    currency_code,
    currency_rate,
    reference,
    remaining_credit,
    fully_paid_on_date,
    type,
    has_attachments
FROM 
  prepayments_raw