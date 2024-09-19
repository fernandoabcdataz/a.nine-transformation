{{ config(
    tags=['normalized', 'xero', 'prepayments']
) }}

WITH prepayments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.PrepaymentID') AS prepayment_id,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.Date') AS prepayment_date,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS subtotal,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        SAFE_CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        JSON_VALUE(data, '$.Reference') AS reference,
        SAFE_CAST(JSON_VALUE(data, '$.RemainingCredit') AS NUMERIC) AS remaining_credit,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.FullyPaidOnDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS fully_paid_on_date,
        SAFE_CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name
    FROM 
        {{ source('raw', 'xero_prepayments') }}
)

SELECT
    prepayment_id,
    type,
    prepayment_date,
    status,
    line_amount_types,
    subtotal,
    total_tax,
    total,
    currency_code,
    currency_rate,
    reference,
    remaining_credit,
    updated_date_utc,
    fully_paid_on_date,
    has_attachments,
    contact_id,
    contact_name,
    ingestion_time
FROM 
  prepayments_raw