{{ config(
    tags=['normalized', 'xero', 'credit_notes']
) }}

WITH credit_notes_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.CreditNoteID') AS credit_note_id,
        JSON_VALUE(data, '$.CreditNoteNumber') AS credit_note_number,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Date'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS date,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.Status') AS status,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS sub_total,
        CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        SAFE_CAST(JSON_VALUE(data, '$.CISDeduction') AS NUMERIC) AS cis_deduction,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.FullyPaidOnDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS fully_paid_on_date,
        JSON_VALUE(data, '$.Reference') AS reference,
        SAFE_CAST(JSON_VALUE(data, '$.SentToContact') AS BOOL) AS sent_to_contact,
        CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        CAST(JSON_VALUE(data, '$.RemainingCredit') AS NUMERIC) AS remaining_credit,
        JSON_VALUE(data, '$.PaymentTerms') AS payment_terms,
        JSON_VALUE(data, '$.BrandingThemeID') AS branding_theme_id,
        SAFE_CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments
    FROM 
        {{ source('raw', 'xero_credit_notes') }}
)

SELECT
    ingestion_time,
    credit_note_id,
    credit_note_number,
    contact_id,
    contact_name
    date,
    type,
    status,
    line_amount_types,
    sub_total,
    total_tax,
    total,
    cis_deduction,
    updated_date_utc,
    currency_code,
    fully_paid_on_date,
    reference,
    sent_to_contact,
    currency_rate,
    remaining_credit,
    payment_terms,
    branding_theme_id,
    has_attachments
FROM 
    credit_notes_raw