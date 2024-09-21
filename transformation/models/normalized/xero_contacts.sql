{{ config(
    tags=['normalized', 'xero', 'contacts']
) }}

WITH contacts_raw AS (
    SELECT DISTINCT
        ingestion_time,
        JSON_VALUE(data, '$.ContactID') AS contact_id,
        JSON_VALUE(data, '$.ContactNumber') AS contact_number,
        JSON_VALUE(data, '$.AccountNumber') AS account_number,
        JSON_VALUE(data, '$.ContactStatus') AS contact_status,
        JSON_VALUE(data, '$.Name') AS name,
        JSON_VALUE(data, '$.FirstName') AS first_name,
        JSON_VALUE(data, '$.LastName') AS last_name,
        JSON_VALUE(data, '$.EmailAddress') AS email_address,
        JSON_VALUE(data, '$.BankAccountDetails') AS bank_account_details,
        JSON_VALUE(data, '$.CompanyNumber') AS company_number,
        JSON_VALUE(data, '$.TaxNumber') AS tax_number,
        JSON_VALUE(data, '$.AccountsReceivableTaxType') AS accounts_receivable_tax_type,
        JSON_VALUE(data, '$.AccountsPayableTaxType') AS accounts_payable_tax_type,
        SAFE_CAST(JSON_VALUE(data, '$.IsSupplier') AS BOOL) AS is_supplier,
        SAFE_CAST(JSON_VALUE(data, '$.IsCustomer') AS BOOL) AS is_customer,
        JSON_VALUE(data, '$.DefaultCurrency') AS default_currency,
        TIMESTAMP_MILLIS(
            CAST(
                SAFE.REGEXP_EXTRACT(JSON_VALUE(data, '$.UpdatedDateUTC'), r'/Date\((\d+)\+\d+\)/') AS INT64
            )
        ) AS updated_date_utc
    FROM 
        {{ source('raw', 'xero_contacts') }}
)

SELECT
    ingestion_time,
    contact_id,
    contact_number,
    account_number,
    contact_status,
    name,
    first_name,
    last_name,
    email_address,
    bank_account_details,
    company_number,
    tax_number,
    accounts_receivable_tax_type,
    accounts_payable_tax_type,
    is_supplier,
    is_customer,
    default_currency,
    updated_date_utc
FROM 
    contacts_raw