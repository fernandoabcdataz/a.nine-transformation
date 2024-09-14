{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        credit_note_value.*,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices') }},
        UNNEST(CreditNotes) AS credit_note_value
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    CAST(CreditNoteID AS STRING) AS credit_note_id,
    CAST(CreditNoteNumber AS STRING) AS credit_note_number,
    CAST(Date AS STRING) AS date_string,
    SAFE.PARSE_DATE('%Y-%m-%d', Date) AS date,
    Total AS total,
    AppliedAmount AS applied_amount,
    HasErrors AS has_errors,
    CAST(ID AS STRING) AS id,
    ingestion_time
FROM
    source