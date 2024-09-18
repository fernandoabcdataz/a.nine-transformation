{{ config(
    tags=['normalized', 'xero', 'contact_batch_payments']
) }}

WITH contact_batch_payments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(data, '$.BatchPayments.BankAccountNumber') AS bank_account_number,
        JSON_VALUE(data, '$.BatchPayments.BankAccountName') AS bank_account_name,
        JSON_VALUE(data, '$.BatchPayments.Details') AS details,
        JSON_VALUE(data, '$.BatchPayments.Code') AS code,
        JSON_VALUE(data, '$.BatchPayments.Reference') AS reference
    FROM 
        {{ source('raw', 'xero_contacts') }}
)

SELECT
    ingestion_time,
    contact_id,
    bank_account_number,
    bank_account_name,
    details,
    code,
    reference
FROM 
    contact_batch_payments_raw