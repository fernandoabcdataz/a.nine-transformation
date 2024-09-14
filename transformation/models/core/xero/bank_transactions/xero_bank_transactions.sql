{{ config(
    materialized='table',
    tags=['normalized', 'xero', 'bank_transactions']
) }}

WITH source AS (
    SELECT
        BankTransactionID AS bank_transaction_id,
        -- flatten BankAccount STRUCT
        BankAccount.AccountID AS bank_account_id,
        BankAccount.Code AS bank_account_code,
        BankAccount.Name AS bank_account_name,
        -- flatten Contact STRUCT
        Contact.ContactID AS contact_id,
        Contact.Name AS contact_name,
        Contact.HasValidationErrors AS contact_has_validation_errors,
        CurrencyCode AS currency_code,
        SAFE.PARSE_DATE('%Y-%m-%d', Date) AS date,
        DateString AS date_string,  -- assuming this is already a TIMESTAMP
        ExternalLinkProviderName AS external_link_provider_name,
        HasAttachments AS has_attachments,
        IsReconciled AS is_reconciled,
        LineAmountTypes AS line_amount_types,
        Reference AS reference,
        Status AS status,
        SubTotal AS sub_total,
        Total AS total,
        TotalTax AS total_tax,
        Type AS type,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', UpdatedDateUTC) AS updated_date_utc,
        Url AS url,
        ingestion_time
    FROM
        {{ source('raw', 'xero_bank_transactions')}}
)

SELECT
    CAST(bank_transaction_id AS STRING) AS bank_transaction_id,
    CAST(bank_account_id AS STRING) AS bank_account_id,
    CAST(bank_account_code AS STRING) AS bank_account_code,
    CAST(bank_account_name AS STRING) AS bank_account_name,
    CAST(contact_id AS STRING) AS contact_id,
    CAST(contact_name AS STRING) AS contact_name,
    CAST(contact_has_validation_errors AS BOOLEAN) AS contact_has_validation_errors,
    CAST(currency_code AS STRING) AS currency_code,
    date,
    date_string,
    CAST(external_link_provider_name AS STRING) AS external_link_provider_name,
    CAST(has_attachments AS BOOLEAN) AS has_attachments,
    CAST(is_reconciled AS BOOLEAN) AS is_reconciled,
    CAST(line_amount_types AS STRING) AS line_amount_types,
    CAST(reference AS STRING) AS reference,
    CAST(status AS STRING) AS status,
    sub_total,
    total,
    total_tax,
    CAST(type AS STRING) AS type,
    updated_date_utc,
    CAST(url AS STRING) AS url,
    ingestion_time
FROM
    source