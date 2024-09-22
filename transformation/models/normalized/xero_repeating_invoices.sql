{{ config(
    tags=['normalized', 'xero', 'repeating_invoices']
) }}

WITH repeating_invoices_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.RepeatingInvoiceID') AS repeating_invoice_id,
        JSON_VALUE(data, '$.Type') AS type,
        JSON_VALUE(data, '$.Contact.ContactID') AS contact_id,
        JSON_VALUE(data, '$.Contact.Name') AS contact_name,
        JSON_QUERY_ARRAY(data, '$.Contact.Addresses') AS contact_addresses,
        JSON_QUERY_ARRAY(data, '$.Contact.ContactGroups') AS contact_contact_groups,
        JSON_QUERY_ARRAY(data, '$.Contact.ContactPersons') AS contact_contact_persons,
        JSON_QUERY_ARRAY(data, '$.Contact.Phones') AS contact_phones,
        JSON_VALUE(data, '$.Contact.HasValidationErrors') AS has_validation_errors,
        JSON_VALUE(data, '$.Schedule.Period') AS schedule_period,
        JSON_VALUE(data, '$.Schedule.Unit') AS schedule_unit,
        JSON_VALUE(data, '$.Schedule.DueDate') AS schedule_due_date,
        JSON_VALUE(data, '$.Schedule.DueDateType') AS schedule_due_date_type,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Schedule.StartDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS schedule_start_date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Schedule.NextScheduledDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS schedule_next_scheduled_date,
        TIMESTAMP_MILLIS(
            CAST(
                REGEXP_EXTRACT(JSON_VALUE(data, '$.Schedule.EndDate'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS schedule_end_date,
        JSON_VALUE(data, '$.LineAmountTypes') AS line_amount_types,
        JSON_VALUE(data, '$.Reference') AS reference,
        JSON_VALUE(data, '$.BrandingThemeID') AS branding_theme_id,
        JSON_VALUE(data, '$.CurrencyCode') AS currency_code,
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
    type,
    contact_id,
    contact_name,
    contact_addresses,
    contact_contact_groups,
    contact_contact_persons,
    contact_phones,
    has_validation_errors,
    schedule_period,
    schedule_unit,
    schedule_due_date,
    schedule_due_date_type,
    schedule_start_date,
    schedule_next_scheduled_date,
    schedule_end_date,
    line_amount_types,
    reference,
    branding_theme_id,
    currency_code,
    status,
    subtotal,
    total_tax,
    total,
    has_attachments,
    approved_for_sending,
    send_copy,
    mark_as_sent,
    include_pdf
FROM 
    repeating_invoices_raw