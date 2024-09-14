{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        payment_value.*,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices') }},
        UNNEST(Payments) AS payment_value
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    CAST(PaymentID AS STRING) AS payment_id,
    CAST(Date AS STRING) AS payment_date_string,
    SAFE.PARSE_DATE('%Y-%m-%d', Date) AS payment_date,
    Amount AS amount,
    CAST(Reference AS STRING) AS reference,
    CurrencyRate AS currency_rate,
    HasAccount AS has_account,
    HasValidationErrors AS has_validation_errors,
    ingestion_time
FROM
    source