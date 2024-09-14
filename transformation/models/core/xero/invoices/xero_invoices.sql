{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        InvoiceNumber AS invoice_number,
        Type AS type,
        Status AS status,
        Reference AS reference,
        BrandingThemeID AS branding_theme_id,
        CurrencyCode AS currency_code,
        CurrencyRate AS currency_rate,
        SAFE.PARSE_DATE('%Y-%m-%d', Date) AS date,
        DateString AS date_string,  -- assuming this is already a TIMESTAMP
        SAFE.PARSE_DATE('%Y-%m-%d', DueDate) AS due_date,
        DueDateString AS due_date_string,
        SAFE.PARSE_DATE('%Y-%m-%d', FullyPaidOnDate) AS fully_paid_on_date,
        LineAmountTypes AS line_amount_types,
        SubTotal AS sub_total,
        TotalTax AS total_tax,
        Total AS total,
        AmountDue AS amount_due,
        AmountPaid AS amount_paid,
        AmountCredited AS amount_credited,
        SentToContact AS sent_to_contact,
        HasAttachments AS has_attachments,
        HasErrors AS has_errors,
        IsDiscounted AS is_discounted,
        RepeatingInvoiceID AS repeating_invoice_id,
        Url AS url,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', UpdatedDateUTC) AS updated_date_utc,
        ingestion_time,
        -- flatten contact STRUCT
        Contact.ContactID AS contact_id,
        Contact.ContactNumber AS contact_number,
        Contact.Name AS contact_name,
        Contact.HasValidationErrors AS contact_has_validation_errors
    FROM
       {{ source('raw', 'xero_invoices')}}
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    CAST(invoice_number AS STRING) AS invoice_number,
    CAST(type AS STRING) AS type,
    CAST(status AS STRING) AS status,
    CAST(reference AS STRING) AS reference,
    CAST(branding_theme_id AS STRING) AS branding_theme_id,
    CAST(currency_code AS STRING) AS currency_code,
    currency_rate,
    date,
    date_string,
    due_date,
    due_date_string,
    fully_paid_on_date,
    CAST(line_amount_types AS STRING) AS line_amount_types,
    sub_total,
    total_tax,
    total,
    amount_due,
    amount_paid,
    amount_credited,
    CAST(sent_to_contact AS BOOLEAN) AS sent_to_contact,
    CAST(has_attachments AS BOOLEAN) AS has_attachments,
    CAST(has_errors AS BOOLEAN) AS has_errors,
    CAST(is_discounted AS BOOLEAN) AS is_discounted,
    CAST(repeating_invoice_id AS STRING) AS repeating_invoice_id,
    CAST(url AS STRING) AS url,
    updated_date_utc,
    ingestion_time,
    CAST(contact_id AS STRING) AS contact_id,
    CAST(contact_number AS STRING) AS contact_number,
    CAST(contact_name AS STRING) AS contact_name,
    CAST(contact_has_validation_errors AS BOOLEAN) AS contact_has_validation_errors
FROM
    source