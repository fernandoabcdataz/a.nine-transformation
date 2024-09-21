{{ config(
    tags=['normalized', 'xero', 'contacts', 'contact__batch_payments']
) }}

WITH contact_batch_payments_raw AS (
    SELECT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(batch_payments, '$.BankAccountNumber') AS bank_account_number,
        JSON_VALUE(batch_payments, '$.BankAccountName') AS bank_account_name,
        JSON_VALUE(batch_payments, '$.Details') AS details,
        JSON_VALUE(batch_payments, '$.Code') AS code,
        JSON_VALUE(batch_payments, '$.Reference') AS reference
    FROM 
        {{ source('raw', 'xero_contacts') }},
        UNNEST(JSON_EXTRACT_ARRAY(data, '$.BatchPayments')) AS batch_payments
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