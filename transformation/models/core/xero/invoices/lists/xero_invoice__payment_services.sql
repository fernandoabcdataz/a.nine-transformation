{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'invoices']
) }}

WITH source AS (
    SELECT
        InvoiceID AS invoice_id,
        payment_service_value,
        ingestion_time
    FROM
        {{ source('raw', 'xero_invoices')}},
        UNNEST(InvoicePaymentServices) AS payment_service_value
)

SELECT
    CAST(invoice_id AS STRING) AS invoice_id,
    -- Parse payment_service_value JSON string
    CAST(JSON_VALUE(payment_service_value, '$.PaymentServiceID') AS STRING) AS payment_service_id,
    CAST(JSON_VALUE(payment_service_value, '$.PaymentServiceName') AS STRING) AS payment_service_name,
    CAST(JSON_VALUE(payment_service_value, '$.PaymentServiceUrl') AS STRING) AS payment_service_url,
    ingestion_time
FROM
    source