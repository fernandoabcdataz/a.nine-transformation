{{ config(
    tags=['normalized', 'xero', 'repeating_invoices']
) }}

WITH repeating_invoices_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.RepeatingInvoiceID') AS repeating_invoice_id,
        JSON_VALUE(data, '$.Type') AS invoice_type,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.NextScheduledDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS next_scheduled_date,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.BrandingThemeID') AS branding_theme_id,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
        SAFE_CAST(JSON_VALUE(data, '$.CurrencyRate') AS NUMERIC) AS currency_rate,
        JSON_VALUE(data, '$.Status') AS status,
        SAFE_CAST(JSON_VALUE(data, '$.SubTotal') AS NUMERIC) AS subtotal,
        SAFE_CAST(JSON_VALUE(data, '$.TotalTax') AS NUMERIC) AS total_tax,
        SAFE_CAST(JSON_VALUE(data, '$.Total') AS NUMERIC) AS total,
        SAFE_CAST(JSON_VALUE(data, '$.HasAttachments') AS BOOL) AS has_attachments,
        SAFE_CAST(JSON_VALUE(data, '$.ApprovedForSending') AS BOOL) AS approved_for_sending,
        SAFE_CAST(JSON_VALUE(data, '$.SendCopy') AS BOOL) AS send_copy,
        SAFE_CAST(JSON_VALUE(data, '$.MarkAsSent') AS BOOL) AS mark_as_sent,
        SAFE_CAST(JSON_VALUE(data, '$.IncludePDF') AS BOOL) AS include_pdf
    FROM 
        {{ source('raw', 'xero_repeating_invoices') }}
)

SELECT
    ingestion_time,
    repeating_invoice_id,
    invoice_type,
    quote_number,
    reference,
    title,
    summary,
    terms,
    parsed_invoice_date AS invoice_date,
    parsed_expiry_date AS expiry_date,
    status,
    line_amount_types,
    subtotal,
    total_tax,
    total,
    total_discount,
    currency_code,
    currency_rate,
    branding_theme_id,
    has_attachments,
    approved_for_sending,
    send_copy,
    mark_as_sent,
    include_pdf,
    contact_id,
    contact_name,
    parsed_start_date AS start_date,
    next_scheduled_date,
    parsed_end_date AS end_date,
    updated_date_utc
FROM 
    repeating_invoices_raw