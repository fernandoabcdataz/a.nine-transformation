{{ config(
    tags=['normalized', 'xero', 'overpayments']
) }}

WITH overpayments_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.OverpaymentID') AS overpayment_id,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS date,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS sub_total,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        SAFE_CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        SAFE_CAST(JSON_VALUE(data, '$.RemainingCredit') AS NUMERIC) AS remaining_credit,
        SAFE_CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments
        -- Allocations
        -- LineItems
        -- Allocations
    FROM 
        {{ source('raw', 'xero_overpayments') }}
)

SELECT
    ingestion_time,
    overpayment_id,
    type,
    contact_id,
    contact_name,
    date,
    status,
    line_amount_types,
    sub_total,
    total_tax,
    total,
    updated_date_utc,
    currency_code,
    currency_rate,
    remaining_credit,
    has_attachments
FROM 
    overpayments_raw